package service

import (
	"context"
	"encoding/json"
	config "flo/assessment/config/circuitbreaker"
	"flo/assessment/config/log"
	"flo/assessment/config/mysql"
	"flo/assessment/config/toml"
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

type chunkJob struct {
	rows      []entity.MeterReadingsEntity
	baseIndex int
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
		BatchSize:         toml.GetConfig().Process.Batchsize,
		InsertTimeout:     30 * time.Second,
		ParseLocation:     loc,
		AllowEmptyReading: false,
		ProgressInterval:  toml.GetConfig().Process.Batchsize / 10,
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
	log.Logger.Info("Entry to process file", zap.String("filePath", filePath))

	// CRITICAL: Enable bulk optimizations before processing
	if err := mysql.EnableBulkOptimizations(); err != nil {
		log.Logger.Warn("Failed to enable bulk optimizations", zap.Error(err))
	}

	// Ensure we restore settings even if processing fails
	defer func() {
		if err := mysql.DisableBulkOptimizations(); err != nil {
			log.Logger.Warn("Failed to restore database settings", zap.Error(err))
		}
	}()

	lowerPath := strings.ToLower(filePath)
	ext := filepath.Ext(lowerPath)

	switch ext {
	case ".zip":
		return IZipProcessService.ProcessZipFile(filePath, p)
	case ".csv", ".mdff":
		err := p.RunPipeline(filePath)
		log.Logger.Info("pipeline finished", zap.String("file", filePath), zap.Error(err))
		return err
		// return ICsvProcessService.ProcessCsvFile(filePath, p)
	default:
		return fmt.Errorf("wrong file type, only csv and zip allowed")
	}
}

// RunPipeline reads a CSV and writes batches concurrently
func (p *FileProcessServiceImpl) RunPipeline(filePath string) error {
	rows := make(chan entity.MeterReadingsEntity, toml.GetConfig().Process.Batchsize*2) // parsed rows
	batches := make(chan []entity.MeterReadingsEntity, p.concurrency*2)

	var wg sync.WaitGroup

	// 1. CSV reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ICsvProcessService.ProcessCsvFileToChannel(filePath, p, rows); err != nil {
			log.Logger.Error("csv processing failed", zap.Error(err))
		}
		close(rows) // no more rows
	}()

	// 2. Batcher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		batch := make([]entity.MeterReadingsEntity, 0, p.opts.BatchSize)
		for row := range rows {
			batch = append(batch, row)
			if len(batch) >= p.opts.BatchSize {
				copyBatch := make([]entity.MeterReadingsEntity, len(batch))
				copy(copyBatch, batch)
				batches <- copyBatch
				batch = batch[:0]
			}
		}
		if len(batch) > 0 {
			batches <- batch
		}
		close(batches) // no more batches
	}()

	// 3. DB writer pool
	for i := 0; i < p.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for batch := range batches {
				ctx, cancel := context.WithTimeout(p.ctx, p.opts.InsertTimeout)
				_db := mysql.GetDB().WithContext(ctx)
				errorRows, err := p.insertBatch(_db, batch)
				cancel()
				if len(errorRows) > 0 {
					p.handleErrorRows(errorRows) // collect errors
				}
				if err != nil {
					log.Logger.Error("DB insert failed", zap.Int("rows", len(batch)), zap.Error(err))
				}
			}
		}(i)
	}

	wg.Wait()
	if err := p.StoreAllErrors(); err != nil {
		log.Logger.Error("storing error rows failed", zap.Error(err))
	}
	return nil
}

func (p *FileProcessServiceImpl) insertBatch(db *gorm.DB, batch []entity.MeterReadingsEntity) ([]BatchError, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	err, backoff := p.attemptBatchInsert(db, batch)
	if err == nil && !backoff {
		return nil, nil
	}

	log.Logger.Warn("batch insert failed, attempting backoff and store", zap.Int("batch_size", len(batch)), zap.Error(err))
	stagingErr, _ := p.backoffandstorefirst(db, batch)
	var batchErrors []BatchError

	if stagingErr != nil {
		for i, b := range batch {
			batchErrors = append(batchErrors, BatchError{
				MeterRow: b,
				Error:    stagingErr,
				Index:    i,
			})
		}
		return batchErrors, stagingErr
	}

	for i, b := range batch {
		batchErrors = append(batchErrors, BatchError{
			MeterRow: b,
			Error:    fmt.Errorf("repeated nmi-timestamp or data integrity issues"),
			Index:    i,
		})
	}

	return batchErrors, err

}

// attemptBatchInsert performs the actual batch insert
func (p *FileProcessServiceImpl) attemptBatchInsert(db *gorm.DB, batch []entity.MeterReadingsEntity) (error, bool) {
	// when it comes here, data should be almost clean already
	// data checking and cleaning should be done at the start of data ingestion

	return config.RetryWithCircuitBreaker(db, func(tx *gorm.DB) error {
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "nmi"}, {Name: "timestamp"}},
			DoUpdates: clause.AssignmentColumns([]string{"consumption", "updated_at"}),
		}).CreateInBatches(batch, p.opts.BatchSize).Error
	}, 3)

}

func (p *FileProcessServiceImpl) backoffandstorefirst(db *gorm.DB, batch []entity.MeterReadingsEntity) (error, bool) {
	stagingBatch := make([]entity.StagingMeterReadingEntity, len(batch))
	for i, row := range batch {
		stagingBatch[i] = entity.StagingMeterReadingEntity{
			Id:               row.Id, // or generate new UUID if needed
			Nmi:              row.Nmi,
			Timestamp:        row.Timestamp,
			Consumption:      row.Consumption,
			FileCreationDate: row.FileCreationDate, // or actual file date
			CreatedAt:        row.CreatedAt,
			UpdatedAt:        row.UpdatedAt,
		}
	}

	return config.RetryWithCircuitBreaker(db, func(tx *gorm.DB) error {
		res := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "nmi"}, {Name: "timestamp"}, {Name: "file_creation_date"}},
			DoUpdates: clause.AssignmentColumns([]string{"consumption", "updated_at"}),
		}).CreateInBatches(stagingBatch, p.opts.BatchSize)

		return res.Error
	}, 3)
}

// func (p *FileProcessServiceImpl) insertBatchWithRetry(db *gorm.DB, batch []entity.MeterReadingsEntity) ([]BatchError, error) {
// 	if len(batch) == 0 {
// 		return nil, nil
// 	}
// 	err := p.attemptBatchInsert(db, batch)
// 	if err == nil {
// 		return nil, nil
// 	}
// 	log.Logger.Warn("batch insert failed, attempting error isolation", zap.Int("batch_size", len(batch)), zap.Error(err))

// 	return p.isolateErrorRows(db, batch)

// }

// func (p *FileProcessServiceImpl) isolateErrorRows(db *gorm.DB, batch []entity.MeterReadingsEntity) ([]BatchError, error) {
// 	var errorRows []BatchError
// 	var successCount int

// 	// Process in chunks to isolate bad rows
// 	errorRows, successCount = p.processChunks(db, batch, 0)

// 	log.Logger.Info("batch processing completed",
// 		zap.Int("total_rows", len(batch)),
// 		zap.Int("successful_rows", successCount),
// 		zap.Int("failed_rows", len(errorRows)))

// 	if len(errorRows) > 0 {
// 		log.Logger.Warn("batch insert failed", zap.Int("rows failed", len(errorRows)))

// 		return errorRows, fmt.Errorf("batch had %d failed rows out of %d total", len(errorRows), len(batch))
// 	}

// 	return nil, nil
// }

// func (p *FileProcessServiceImpl) processChunks(db *gorm.DB, batch []entity.MeterReadingsEntity, baseIndex int) ([]BatchError, int) {
// 	var errorRows []BatchError
// 	var totalSuccess int32

// 	chunkCh := make(chan chunkJob, len(batch))
// 	resultCh := make(chan BatchError, len(batch))
// 	var wg sync.WaitGroup

// 	// Seed the first job
// 	chunkCh <- chunkJob{rows: batch, baseIndex: baseIndex}
// 	close(chunkCh) // we'll refill it dynamically in the worker

// 	worker := func() {
// 		for job := range chunkCh {
// 			err := p.attemptBatchInsert(db, job.rows)
// 			if err != nil && len(job.rows) > 1 {
// 				// split into two halves
// 				mid := len(job.rows) / 2
// 				// push new chunks into a local slice, which we send to channel after
// 				chunkCh <- chunkJob{rows: job.rows[:mid], baseIndex: job.baseIndex}
// 				chunkCh <- chunkJob{rows: job.rows[mid:], baseIndex: job.baseIndex + mid}
// 			} else if err != nil {
// 				resultCh <- BatchError{
// 					MeterRow: job.rows[0],
// 					Index:    job.baseIndex,
// 					Error:    err,
// 				}
// 			} else {
// 				atomic.AddInt32(&totalSuccess, int32(len(job.rows)))
// 			}
// 		}
// 	}

// 	// Start workers
// 	concurrency := p.concurrency
// 	wg.Add(concurrency)
// 	for i := 0; i < concurrency; i++ {
// 		go func() {
// 			defer wg.Done()
// 			worker()
// 		}()
// 	}

// 	wg.Wait()
// 	close(resultCh)

// 	// Collect all errors
// 	for e := range resultCh {
// 		errorRows = append(errorRows, e)
// 	}

// 	return errorRows, int(totalSuccess)
// }

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
		errorMsg := ""
		if errRow.Error != nil {
			errorMsg = errRow.Error.Error()
		}
		errorRecords = append(errorRecords, entity.ErrorRowsEntity{
			JobID:    int64(p.workerJobId),
			Data:     dataJSON,
			FilePath: p.jobPath,
			Error:    errorMsg,
		})
	}

	if len(errorRecords) == 0 {
		return nil
	}

	config.RetryWithCircuitBreaker(db, func(tx *gorm.DB) error {
		return tx.CreateInBatches(errorRecords, 100).Error
	}, 3)

	p.jobErrors = p.jobErrors[:0]

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
