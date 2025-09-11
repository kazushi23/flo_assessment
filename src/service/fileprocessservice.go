package service

import (
	"context"
	"encoding/json"
	"flo/assessment/config/log"
	"flo/assessment/config/mysql"
	"flo/assessment/entity"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type FileProcessServiceImpl struct {
	ctx         context.Context
	opts        *ProcessorOptions
	batch       []entity.MeterReadingsEntity
	jobErrors   []BatchError
	batchMutex  sync.Mutex
	concurrency int
	sem         chan struct{}
	workerJobId int
	jobPath     string
}

type BatchError struct {
	MeterRow entity.MeterReadingsEntity
	CsvRow   []string
	Error    error
	Index    int
}

// NewZipProcessServiceImpl creates a new service
func NewFileProcessServiceImpl(ctx context.Context, concurrency int, opts *ProcessorOptions, workerJobId int, jobPath string) *FileProcessServiceImpl {
	if opts == nil {
		opts = DefaultProcessorOptions()
	}
	return &FileProcessServiceImpl{
		ctx:         ctx,
		opts:        opts,
		concurrency: concurrency,
		batch:       make([]entity.MeterReadingsEntity, 0, opts.BatchSize),
		sem:         make(chan struct{}, concurrency),
		workerJobId: workerJobId,
		jobPath:     jobPath,
	}
}

// ProcessorOptions defines options for CSV processing and batching
type ProcessorOptions struct {
	BatchSize         int           // rows per DB batch
	InsertTimeout     time.Duration // timeout for DB insert
	ParseLocation     *time.Location
	AllowEmptyReading bool // insert 0 for empty values if true
	ProgressInterval  int
}

// DefaultProcessorOptions returns sane defaults
func DefaultProcessorOptions() *ProcessorOptions {
	loc, _ := time.LoadLocation("UTC")
	return &ProcessorOptions{
		BatchSize:         1000,
		InsertTimeout:     30 * time.Second,
		ParseLocation:     loc,
		AllowEmptyReading: false,
		ProgressInterval:  10000,
	}
}

// checkContextCancelled returns an error if context is done
func (p *FileProcessServiceImpl) checkContextCancelled() error {
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	default:
		return nil
	}
}

func (p *FileProcessServiceImpl) ProcessFileEntry(filePath string) error {
	log.Logger.Info("Entry to process zip file", zap.String("zipPath", filePath))

	lowerPath := strings.ToLower(filePath)
	ext := filepath.Ext(lowerPath)

	switch ext {
	case ".zip":
		return IZipProcessService.ProcessZipFile(filePath, p)
	case ".csv":
		return ICsvProcessService.ProcessCsvFile(filePath, p)
	default:
		return fmt.Errorf("wrong file type, only csv and zip allowed")
	}
}

// addToBatch appends row to batch safely and flushes if batch size reached
func (p *FileProcessServiceImpl) addToBatch(row entity.MeterReadingsEntity) {
	p.batchMutex.Lock()
	p.batch = append(p.batch, row)
	flushNeeded := len(p.batch) >= p.opts.BatchSize
	p.batchMutex.Unlock()

	if flushNeeded {
		if err := p.flushBatch(); err != nil {
			log.Logger.Error("flush batch failed", zap.Error(err))
		}
	}
}

// flushBatch inserts batch into DB safely
func (p *FileProcessServiceImpl) flushBatch() error {
	p.batchMutex.Lock()
	if len(p.batch) == 0 {
		p.batchMutex.Unlock()
		return nil
	}

	batchCopy := make([]entity.MeterReadingsEntity, len(p.batch))
	copy(batchCopy, p.batch)
	p.batch = p.batch[:0]
	p.batchMutex.Unlock()

	ctx, cancel := context.WithTimeout(p.ctx, p.opts.InsertTimeout)
	defer cancel()

	_db := mysql.GetDB().WithContext(ctx)

	errorRows, err := p.insertBatchWithRetry(_db, batchCopy)

	// Store error rows for later inspection/reporting
	if len(errorRows) > 0 {
		p.handleErrorRows(errorRows)
	}

	if err != nil {
		log.Logger.Error("batch insert completed with errors", zap.Int("total_rows", len(batchCopy)), zap.Int("failed_rows", len(errorRows)), zap.Error(err))
	}

	return nil
}

func (p *FileProcessServiceImpl) insertBatchWithRetry(db *gorm.DB, batch []entity.MeterReadingsEntity) ([]BatchError, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	err := p.attemptBatchInsert(db, batch)
	if err == nil {
		return nil, nil
	}
	log.Logger.Warn("batch insert failed, attempting error isolation", zap.Int("batch_size", len(batch)), zap.Error(err))

	return p.isolateErrorRows(db, batch)

}

// attemptBatchInsert performs the actual batch insert
func (p *FileProcessServiceImpl) attemptBatchInsert(db *gorm.DB, batch []entity.MeterReadingsEntity) error {
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "nmi"}, {Name: "timestamp"}},
		DoUpdates: clause.AssignmentColumns([]string{"consumption", "updated_at"}),
	}).CreateInBatches(batch, p.opts.BatchSize).Error
}

func (p *FileProcessServiceImpl) isolateErrorRows(db *gorm.DB, batch []entity.MeterReadingsEntity) ([]BatchError, error) {
	var errorRows []BatchError
	var successCount int

	// Process in chunks to isolate bad rows
	errorRows, successCount = p.processChunks(db, batch, 0)

	log.Logger.Info("batch processing completed",
		zap.Int("total_rows", len(batch)),
		zap.Int("successful_rows", successCount),
		zap.Int("failed_rows", len(errorRows)))

	if len(errorRows) > 0 {
		return errorRows, fmt.Errorf("batch had %d failed rows out of %d total", len(errorRows), len(batch))
	}

	return nil, nil
}

func (p *FileProcessServiceImpl) processChunks(db *gorm.DB, batch []entity.MeterReadingsEntity, baseIndex int) ([]BatchError, int) {
	if len(batch) == 0 {
		return nil, 0
	}

	// Try the chunk
	err := p.attemptBatchInsert(db, batch)
	if err == nil {
		return nil, len(batch) // All rows in this chunk succeeded
	}

	// If single row, it's the problem
	if len(batch) == 1 {
		return []BatchError{{
			MeterRow: batch[0],
			Error:    err,
			Index:    baseIndex,
		}}, 0
	}

	// Split batch in half and recurse
	mid := len(batch) / 2
	leftBatch := batch[:mid]
	rightBatch := batch[mid:]

	leftErrors, leftSuccess := p.processChunks(db, leftBatch, baseIndex)
	rightErrors, rightSuccess := p.processChunks(db, rightBatch, baseIndex+mid)

	// Combine results
	allErrors := append(leftErrors, rightErrors...)
	totalSuccess := leftSuccess + rightSuccess

	return allErrors, totalSuccess
}

func (p *FileProcessServiceImpl) handleErrorRows(errorRows []BatchError) {
	if len(errorRows) == 0 {
		return
	}
	p.jobErrors = append(p.jobErrors, errorRows...)
}
func (p *FileProcessServiceImpl) StoreAllErrors() error {
	if len(p.jobErrors) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(p.ctx, p.opts.InsertTimeout)
	defer cancel()

	db := mysql.GetDB().WithContext(ctx)

	// Prepare all error records for batch insert
	errorRecords := make([]entity.ErrorRowsEntity, 0, len(p.jobErrors))

	for _, errRow := range p.jobErrors {
		var dataJSON json.RawMessage
		var err error

		// Handle both CSV and MeterRow errors
		if errRow.MeterRow.Nmi != "" {
			// Database insert error - marshal MeterRow
			dataJSON, err = json.Marshal(errRow.MeterRow)
		} else if len(errRow.CsvRow) > 0 {
			// CSV parsing error - marshal raw CSV row
			dataJSON, err = json.Marshal(errRow.CsvRow)
		}

		if err != nil {
			log.Logger.Warn("failed to marshal error row data", zap.Error(err))
			continue // Skip this error row but continue with others
		}

		errorRecords = append(errorRecords, entity.ErrorRowsEntity{
			JobID:    int64(p.workerJobId),
			Data:     dataJSON,
			FilePath: p.jobPath,
			Error:    errRow.Error.Error(),
		})
	}

	// Batch insert all error records
	if len(errorRecords) > 0 {
		if err := db.CreateInBatches(errorRecords, 100).Error; err != nil {
			log.Logger.Error("failed to batch store error records", zap.Error(err))
			return err
		}
	}

	log.Logger.Info("Stored job errors", zap.Int("error_count", len(errorRecords)))
	return nil
}

func (p *FileProcessServiceImpl) addErrorRowCSV(fields []string, err error) {
	if p.jobErrors == nil {
		p.jobErrors = make([]BatchError, 0)
	}
	p.jobErrors = append(p.jobErrors, BatchError{
		CsvRow: fields, // raw CSV row
		Error:  err,
	})
}
