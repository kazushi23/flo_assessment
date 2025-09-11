package service

import (
	"context"
	"encoding/json"
	config "flo/assessment/config/circuitBreaker"
	"flo/assessment/config/log"
	"flo/assessment/config/mysql"
	"flo/assessment/config/toml"
	"flo/assessment/entity"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
				errorRows, err := p.insertBatchWithRetry(_db, batch)
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

// // addToBatch appends row to batch safely and flushes if batch size reached
// func (p *FileProcessServiceImpl) addToBatch(row entity.MeterReadingsEntity) {
// 	p.batchMutex.Lock()
// 	p.batch = append(p.batch, row)
// 	flushNeeded := len(p.batch) >= p.opts.BatchSize
// 	p.batchMutex.Unlock()

// 	if flushNeeded {
// 		if err := p.flushBatch(); err != nil {
// 			log.Logger.Error("flush batch failed", zap.Error(err))
// 		}
// 	}
// }

// flushBatch inserts batch into DB safely
// func (p *FileProcessServiceImpl) flushBatch() error {
// 	p.batchMutex.Lock()
// 	if len(p.batch) == 0 {
// 		p.batchMutex.Unlock()
// 		return nil
// 	}

// 	batchCopy := make([]entity.MeterReadingsEntity, len(p.batch))
// 	copy(batchCopy, p.batch)
// 	p.batch = p.batch[:0]
// 	p.batchMutex.Unlock()

// 	p.sem <- struct{}{}        // acquire slot
// 	defer func() { <-p.sem }() // release slot

// 	// go func() {
// 	ctx, cancel := context.WithTimeout(context.Background(), p.opts.InsertTimeout)
// 	defer cancel()

// 	_db := mysql.GetDB().WithContext(ctx)
// 	errorRows, err := p.insertBatchWithRetry(_db, batchCopy)

// 	// Store error rows for later inspection/reporting
// 	if len(errorRows) > 0 {
// 		p.handleErrorRows(errorRows)
// 	}

// 	if err != nil {
// 		log.Logger.Error("batch insert completed with errors", zap.Int("total_rows", len(batchCopy)), zap.Int("failed_rows", len(errorRows)), zap.Error(err))
// 	}
// 	// }()

// 	// ctx, cancel := context.WithTimeout(p.ctx, p.opts.InsertTimeout)
// 	// defer cancel()

// 	// _db := mysql.GetDB().WithContext(ctx)

// 	// errorRows, err := p.insertBatchWithRetry(_db, batchCopy)

// 	// // Store error rows for later inspection/reporting
// 	// if len(errorRows) > 0 {
// 	// 	p.handleErrorRows(errorRows)
// 	// }

// 	// if err != nil {
// 	// 	log.Logger.Error("batch insert completed with errors", zap.Int("total_rows", len(batchCopy)), zap.Int("failed_rows", len(errorRows)), zap.Error(err))
// 	// }

// 	return nil
// }

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
	return config.RetryWithCircuitBreaker(db, func(tx *gorm.DB) error {
		res := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "nmi"}, {Name: "timestamp"}},
			DoUpdates: clause.AssignmentColumns([]string{"consumption", "updated_at"}),
		}).CreateInBatches(batch, p.opts.BatchSize)

		if res.Error != nil {
			log.Logger.Error("batch insert error", log.Any("err", res.Error))
		}

		return res.Error
	}, 3)
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
		log.Logger.Warn("batch insert failed", zap.Int("rows failed", len(errorRows)))

		return errorRows, fmt.Errorf("batch had %d failed rows out of %d total", len(errorRows), len(batch))
	}

	return nil, nil
}

func (p *FileProcessServiceImpl) processChunks(db *gorm.DB, batch []entity.MeterReadingsEntity, baseIndex int) ([]BatchError, int) {
	var (
		errorRows    []BatchError
		errorMutex   sync.Mutex
		totalSuccess int32 // use atomic increment
	)

	queue := []chunkJob{{rows: batch, baseIndex: 0}}
	sem := make(chan struct{}, p.concurrency) // limit concurrent chunk inserts
	var wg sync.WaitGroup

	for len(queue) > 0 {
		job := queue[0]
		queue = queue[1:]

		if len(job.rows) == 0 {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(job chunkJob) {
			defer wg.Done()
			defer func() { <-sem }()

			err := p.attemptBatchInsert(db, job.rows)
			if err != nil && len(job.rows) > 1 {
				// split chunk
				mid := len(job.rows) / 2
				queue = append(queue, chunkJob{rows: job.rows[:mid], baseIndex: job.baseIndex})
				queue = append(queue, chunkJob{rows: job.rows[mid:], baseIndex: job.baseIndex + mid})
			} else if err != nil {
				// record failed row
				errorMutex.Lock()
				errorRows = append(errorRows, BatchError{
					MeterRow: job.rows[0],
					Index:    job.baseIndex,
					Error:    err,
				})
				errorMutex.Unlock()
			} else {
				// successful rows
				atomic.AddInt32(&totalSuccess, int32(len(job.rows)))
			}
		}(job)
	}

	wg.Wait()
	return errorRows, int(totalSuccess)

	// var errorRows []BatchError
	// var totalSuccess int

	// queue := []chunkJob{{rows: batch, baseIndex: 0}}

	// for len(queue) > 0 {
	// 	job := queue[0]
	// 	queue = queue[1:]

	// 	if len(job.rows) == 0 {
	// 		continue
	// 	}

	// 	err := p.attemptBatchInsert(db, job.rows)
	// 	if err == nil {
	// 		totalSuccess += len(job.rows)
	// 		continue
	// 	}

	// 	if len(job.rows) == 1 {
	// 		errorRows = append(errorRows, BatchError{
	// 			MeterRow: job.rows[0],
	// 			Error:    err,
	// 			Index:    job.baseIndex,
	// 		})
	// 		continue
	// 	}

	// 	// Split into two halves and push to queue
	// 	mid := len(job.rows) / 2
	// 	queue = append(queue, chunkJob{rows: job.rows[:mid], baseIndex: job.baseIndex})
	// 	queue = append(queue, chunkJob{rows: job.rows[mid:], baseIndex: job.baseIndex + mid})
	// }

	// return errorRows, totalSuccess
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
