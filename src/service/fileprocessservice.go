package service

import (
	"context"
	"flo/assessment/config/log"
	"flo/assessment/config/mysql"
	"flo/assessment/entity"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm/clause"
)

type FileProcessServiceImpl struct {
	ctx         context.Context
	opts        *ProcessorOptions
	batch       []entity.MeterReadingsEntity
	batchMutex  sync.Mutex
	concurrency int
	sem         chan struct{}
}

// NewZipProcessServiceImpl creates a new service
func NewFileProcessServiceImpl(ctx context.Context, concurrency int, opts *ProcessorOptions) *FileProcessServiceImpl {
	if opts == nil {
		opts = DefaultProcessorOptions()
	}
	return &FileProcessServiceImpl{
		ctx:         ctx,
		opts:        opts,
		concurrency: concurrency,
		batch:       make([]entity.MeterReadingsEntity, 0, opts.BatchSize),
		sem:         make(chan struct{}, concurrency),
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

	// Insert batch with conflict handling
	err := _db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "nmi"}, {Name: "timestamp"}},             // conflict keys
		DoUpdates: clause.AssignmentColumns([]string{"consumption", "updated_at"}), // columns to update
	}).CreateInBatches(batchCopy, p.opts.BatchSize).Error

	if err != nil {
		log.Logger.Error("failed to insert batch", zap.Error(err))
		return err
	}

	return nil
}
