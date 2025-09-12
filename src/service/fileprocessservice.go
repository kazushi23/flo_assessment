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

// one instance utilise from start to end for one parallel data processing flow
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

// when db insertion fail, entire batch will push into errorrowsentity
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

// entry point to kickstart data processing
func (p *FileProcessServiceImpl) ProcessFileEntry(filePath string) error {
	log.Logger.Info("Entry to process file", log.Any("filePath", filePath))

	// Enable bulk optimizations before processing (further optimisation can be done)
	if err := mysql.EnableBulkOptimizations(); err != nil {
		log.Logger.Warn("Failed to enable bulk optimizations", log.Any("error", err))
	}

	// Ensure we restore settings even if processing fails
	defer func() {
		if err := mysql.DisableBulkOptimizations(); err != nil {
			log.Logger.Warn("Failed to restore database settings", log.Any("error", err))
		}
	}()

	lowerPath := strings.ToLower(filePath)
	ext := filepath.Ext(lowerPath)

	switch ext {
	case ".zip": // for zip file (not really in play at the moment)
		return IZipProcessService.ProcessZipFile(filePath, p)
	case ".csv", ".mdff": // for csv and mdff file (main processing path)
		err := p.RunPipeline(filePath)
		log.Logger.Info("pipeline finished", log.Any("file", filePath), log.Any("error", err))
		return err
	default:
		err := fmt.Errorf("wrong file type, only csv and zip allowed")
		log.Logger.Info("unable to handle file", log.Any("error", err))
		return err
	}
}

// RunPipeline reads a CSV and writes batches concurrently
func (p *FileProcessServiceImpl) RunPipeline(filePath string) error {
	rows := make(chan entity.MeterReadingsEntity, toml.GetConfig().Process.Batchsize*2) // parsed rows
	batches := make(chan []entity.MeterReadingsEntity, p.concurrency*2)

	var wg sync.WaitGroup

	// CSV reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		// process csv (200/300/etc)
		if err := ICsvProcessService.ProcessCsvFileToChannel(filePath, p, rows); err != nil {
			log.Logger.Error("csv processing failed", log.Any("error", err))
		}
		close(rows) // no more rows
	}()

	// Batch goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		// init based on batch size configuration
		batch := make([]entity.MeterReadingsEntity, 0, p.opts.BatchSize)
		for row := range rows {
			batch = append(batch, row)
			if len(batch) >= p.opts.BatchSize {
				// copy and release to prevent holding on batch for too long
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

	// DB writer pool
	for i := 0; i < p.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for batch := range batches {
				ctx, cancel := context.WithTimeout(p.ctx, p.opts.InsertTimeout)
				_db := mysql.GetDB().WithContext(ctx)
				// insert into db the data
				errorRows, err := p.insertBatch(_db, batch)
				cancel()
				if len(errorRows) > 0 {
					p.handleErrorRows(errorRows) // collect errors
				}
				if err != nil {
					log.Logger.Error("DB insert failed", zap.Int("rows", len(batch)), log.Any("error", err))
				}
			}
		}(i)
	}

	wg.Wait() // after all 3 goroutines are done, store all errors that occurred
	if err := p.StoreAllErrors(); err != nil {
		log.Logger.Error("storing error rows failed", log.Any("error", err))
	}
	return nil
}

// attempt to write to db, if failed, write to retry table (stagingmeterentity)
func (p *FileProcessServiceImpl) insertBatch(db *gorm.DB, batch []entity.MeterReadingsEntity) ([]BatchError, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	err, backoff := p.attemptBatchInsert(db, batch) // bulk insert into db
	if err == nil && !backoff {                     // all is good, no error, no backoff
		return nil, nil
	}

	log.Logger.Warn("batch insert failed, attempting backoff and store", zap.Int("batch_size", len(batch)), log.Any("error", err))
	stagingErr, _ := p.backoffandstorefirst(db, batch) // backoff needed, push data to staging table
	var batchErrors []BatchError

	if stagingErr != nil { // retrieve all batch errors
		for i, b := range batch {
			batchErrors = append(batchErrors, BatchError{
				MeterRow: b,
				Error:    stagingErr,
				Index:    i,
			})
		}
		return batchErrors, stagingErr
	}

	for i, b := range batch { // retrieve all batch errors
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

// safety mechanism, batchinsert focus on speed and performance
// anything do wrong, go into safe mode (staging table + future single thread processing)
func (p *FileProcessServiceImpl) backoffandstorefirst(db *gorm.DB, batch []entity.MeterReadingsEntity) (error, bool) {
	stagingBatch := make([]entity.StagingMeterReadingEntity, len(batch))
	for i, row := range batch {
		stagingBatch[i] = entity.StagingMeterReadingEntity{
			Id:               row.Id,
			Nmi:              row.Nmi,
			Timestamp:        row.Timestamp,
			Consumption:      row.Consumption,
			FileCreationDate: row.FileCreationDate,
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

// add batch error to p.jobErrors
func (p *FileProcessServiceImpl) handleErrorRows(errorRows []BatchError) {
	if len(errorRows) == 0 {
		return
	}
	p.jobErrors = append(p.jobErrors, errorRows...)
}

// add csv fields to batcherror to p.joberror
func (p *FileProcessServiceImpl) addErrorRowCSV(fields []string, err error) {
	if p.jobErrors == nil {
		p.jobErrors = make([]BatchError, 0)
	}
	p.jobErrors = append(p.jobErrors, BatchError{
		CsvRow: fields, // raw CSV row
		Error:  err,
	})
}

// store all errors
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
			log.Logger.Warn("failed to marshal error row data", log.Any("error", err))
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
	// write to db
	config.RetryWithCircuitBreaker(db, func(tx *gorm.DB) error {
		return tx.CreateInBatches(errorRecords, 100).Error
	}, 3)

	p.jobErrors = p.jobErrors[:0] // clear out

	log.Logger.Info("Stored job errors", zap.Int("error_count", len(errorRecords)))
	return nil
}

// func (p *FileProcessServiceImpl) insertBatchWithRetry(db *gorm.DB, batch []entity.MeterReadingsEntity) ([]BatchError, error) {
// 	if len(batch) == 0 {
// 		return nil, nil
// 	}
// 	err := p.attemptBatchInsert(db, batch)
// 	if err == nil {
// 		return nil, nil
// 	}
// 	log.Logger.Warn("batch insert failed, attempting error isolation", zap.Int("batch_size", len(batch)), log.Any(err))

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
