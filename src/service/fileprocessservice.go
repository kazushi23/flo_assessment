package service

import (
	"archive/zip"
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"flo/assessment/config/log"
	"flo/assessment/config/mysql"
	"flo/assessment/entity"
	"flo/assessment/src/tools"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ProcessorOptions defines options for CSV processing and batching
type ProcessorOptions struct {
	BatchSize         int           // rows per DB batch
	InsertTimeout     time.Duration // timeout for DB insert
	ParseLocation     *time.Location
	AllowEmptyReading bool // insert 0 for empty values if true
}

// DefaultProcessorOptions returns sane defaults
func DefaultProcessorOptions() *ProcessorOptions {
	loc, _ := time.LoadLocation("UTC")
	return &ProcessorOptions{
		BatchSize:         500,
		InsertTimeout:     30 * time.Second,
		ParseLocation:     loc,
		AllowEmptyReading: false,
	}
}

// FileProcessServiceImpl manages NEM12 processing
type FileProcessServiceImpl struct {
	ctx         context.Context
	opts        *ProcessorOptions
	batch       []entity.MeterReadingsEntity
	batchMutex  sync.Mutex
	wg          sync.WaitGroup
	concurrency int
}

// NewFileProcessServiceImpl creates a new service
func NewFileProcessServiceImpl(ctx context.Context, concurrency int, opts *ProcessorOptions) *FileProcessServiceImpl {
	if opts == nil {
		opts = DefaultProcessorOptions()
	}
	return &FileProcessServiceImpl{
		ctx:         ctx,
		opts:        opts,
		concurrency: concurrency,
		batch:       make([]entity.MeterReadingsEntity, 0, opts.BatchSize),
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

// ProcessZipFile reads a zip and processes all entries concurrently
func (p *FileProcessServiceImpl) ProcessZipFile(zipPath string) error {
	log.Logger.Info("Processing zip file", zap.String("zipPath", zipPath))

	r, err := zip.OpenReader(zipPath)
	if err != nil {
		log.Logger.Error("failed to open zip", zap.String("zipPath", zipPath), zap.Error(err))
		return fmt.Errorf("open zip %s: %w", zipPath, err)
	}
	defer r.Close()

	sem := make(chan struct{}, p.concurrency) // semaphore for concurrency

	for _, f := range r.File {
		if p.checkContextCancelled() != nil {
			return p.ctx.Err()
		}

		if f.FileInfo().IsDir() {
			continue
		}

		sem <- struct{}{}
		p.wg.Add(1)
		go func(f *zip.File) {
			defer p.wg.Done()
			defer func() { <-sem }()

			if err := p.ProcessZipEntry(f); err != nil {
				log.Logger.Error("failed to process zip entry", zap.String("entry", f.Name), zap.Error(err))
			}
		}(f)
	}

	p.wg.Wait() // wait for all files to finish

	// Flush any remaining batch
	if err := p.flushBatch(); err != nil {
		log.Logger.Error("final batch flush failed", zap.Error(err))
		return err
	}

	return nil
}

// ProcessZipEntry handles a single file entry in the zip
func (p *FileProcessServiceImpl) ProcessZipEntry(f *zip.File) error {
	rc, err := f.Open()
	if err != nil {
		log.Logger.Warn("failed to open zip entry", zap.String("entry", f.Name), zap.Error(err))
		return err
	}
	defer rc.Close()

	name := strings.ToLower(f.Name)
	switch {
	case strings.HasSuffix(name, ".zip"):
		return p.processNestedZip(rc, name)
	case strings.HasSuffix(name, ".csv"):
		return p.processCSVStream(rc, f.Name)
	default:
		log.Logger.Info("skipping unsupported file", zap.String("entry", f.Name))
	}
	return nil
}

// processNestedZip writes nested zip to temp file and processes recursively
func (p *FileProcessServiceImpl) processNestedZip(rc io.Reader, name string) error {
	tmpFile, err := os.CreateTemp("", "nested-*.zip")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, rc); err != nil {
		return err
	}

	return p.ProcessZipFile(tmpFile.Name())
}

// processCSVStream reads CSV and processes data in batches
func (p *FileProcessServiceImpl) processCSVStream(r io.Reader, sourceName string) error {
	csvr := csv.NewReader(bufio.NewReader(r))
	csvr.TrimLeadingSpace = true
	csvr.LazyQuotes = true
	csvr.FieldsPerRecord = -1

	var currentNMI string
	var currentInterval int
	var recordCount int

	for {
		if err := p.checkContextCancelled(); err != nil {
			return err
		}

		fields, err := csvr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Logger.Info("CSV EOF reached", zap.String("source", sourceName), zap.Int("records", recordCount))
				return p.flushBatch()
			}
			log.Logger.Error("CSV read error", zap.String("source", sourceName), zap.Int("record", recordCount), zap.Error(err))
			return fmt.Errorf("csv read: %w", err)
		}

		if len(fields) == 0 {
			continue
		}
		recordCount++
		recordIndicator := strings.TrimSpace(fields[0])

		switch recordIndicator {
		case "200":
			nmi, interval, ok := p.handle200(fields, sourceName, recordCount)
			if ok {
				currentNMI = nmi
				currentInterval = interval
			} else {
				currentNMI = ""
				currentInterval = 0
			}
		case "300":
			if currentNMI == "" || currentInterval == 0 {
				log.Logger.Warn("300 without valid 200 context", zap.String("source", sourceName), zap.Int("record", recordCount))
				continue
			}
			if err := p.handle300(fields, currentNMI, currentInterval, sourceName, recordCount); err != nil {
				return err
			}
		case "900":
			if err := p.flushBatch(); err != nil {
				return err
			}
			return nil
		default:
			// ignore unknown records
		}
	}
}

// handle200 parses a 200 record
func (p *FileProcessServiceImpl) handle200(fields []string, source string, record int) (string, int, bool) {
	if len(fields) <= 8 {
		log.Logger.Warn("malformed 200 record", zap.String("source", source), zap.Int("record", record))
		return "", 0, false
	}
	nmi := strings.TrimSpace(fields[1])
	ivalStr := strings.TrimSpace(fields[8])
	if ivalStr == "" {
		return nmi, 0, false
	}
	iv, err := strconv.Atoi(ivalStr)
	if err != nil {
		return nmi, 0, false
	}
	return nmi, iv, true
}

// handle300 parses 300 record and adds data to batch
func (p *FileProcessServiceImpl) handle300(fields []string, currentNMI string, currentInterval int, source string, record int) error {
	if len(fields) < 3 {
		return nil
	}

	dateStr := strings.TrimSpace(fields[1])
	intervalDate, err := p.ParseIntervalDates(dateStr)
	if err != nil {
		return nil
	}

	intervalsPerDay := 1440 / currentInterval
	availableValues := len(fields) - 2
	numValues := intervalsPerDay
	if availableValues < intervalsPerDay {
		numValues = availableValues
	}

	for i := 0; i < numValues; i++ {
		if err := p.checkContextCancelled(); err != nil {
			return err
		}

		raw := strings.TrimSpace(fields[2+i])
		if raw == "" && !p.opts.AllowEmptyReading {
			continue
		} else if raw == "" {
			raw = "0"
		}

		val, err := strconv.ParseFloat(strings.ReplaceAll(raw, ",", ""), 64)
		if err != nil {
			continue
		}

		ts := intervalDate.Add(time.Duration(i*currentInterval) * time.Minute)

		p.addToBatch(entity.MeterReadingsEntity{
			Id:          tools.NewUuid(),
			Nmi:         currentNMI,
			Timestamp:   ts,
			Consumption: val,
		})
	}
	return nil
}

// ParseIntervalDates parses multiple date formats
func (p *FileProcessServiceImpl) ParseIntervalDates(s string) (time.Time, error) {
	formats := []string{"20060102", "20060102150405", "200601021504"}
	var t time.Time
	var err error
	for _, f := range formats {
		t, err = time.ParseInLocation(f, s, time.UTC)
		if err == nil {
			return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
		}
	}
	return t, err
}

// addToBatch appends row to batch safely and flushes if batch size reached
func (p *FileProcessServiceImpl) addToBatch(row entity.MeterReadingsEntity) {
	p.batchMutex.Lock()
	defer p.batchMutex.Unlock()

	p.batch = append(p.batch, row)
	if len(p.batch) >= p.opts.BatchSize {
		if err := p.flushBatch(); err != nil {
			log.Logger.Error("flush batch failed", zap.Error(err))
		}
	}
}

// flushBatch inserts batch into DB safely
func (p *FileProcessServiceImpl) flushBatch() error {
	_db := mysql.GetDB()
	p.batchMutex.Lock()
	defer p.batchMutex.Unlock()

	if len(p.batch) == 0 {
		return nil
	}

	batchSize := p.opts.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}

	if err := _db.CreateInBatches(p.batch, batchSize).Error; err != nil {
		log.Logger.Error("failed to insert batch", zap.Error(err))
		return err
	}

	p.batch = p.batch[:0]
	return nil
}
