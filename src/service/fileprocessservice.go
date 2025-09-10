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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm/clause"
)

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

// FileProcessServiceImpl manages NEM12 processing
type FileProcessServiceImpl struct {
	ctx         context.Context
	opts        *ProcessorOptions
	batch       []entity.MeterReadingsEntity
	batchMutex  sync.Mutex
	concurrency int
	sem         chan struct{}
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
		sem:         make(chan struct{}, concurrency),
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
		return p.ProcessZipFile(filePath)
	case ".csv":
		return p.ProcessCsvFile(filePath)
	default:
		return fmt.Errorf("wrong file type, only csv and zip allowed")
	}
}

// ProcessZipFile reads a zip and processes all entries concurrently
func (p *FileProcessServiceImpl) ProcessZipFile(filePath string) error {
	r, err := zip.OpenReader(filePath)
	if err != nil {
		return fmt.Errorf("open zip %s: %w", filePath, err)
	}
	defer r.Close()

	errCh := make(chan error, len(r.File))
	var wg sync.WaitGroup

	for _, f := range r.File {
		if p.checkContextCancelled() != nil {
			return p.ctx.Err()
		}

		if f.FileInfo().IsDir() {
			continue
		}

		p.sem <- struct{}{}
		wg.Add(1)
		go func(f *zip.File) {
			defer wg.Done()
			defer func() { <-p.sem }()

			if err := p.ProcessZipEntry(f); err != nil {
				select {
				case errCh <- fmt.Errorf("entry %s: %w", f.Name, err):
				default:
					log.Logger.Warn("error channel full, skipping error")
				}
			}
		}(f)
	}

	wg.Wait() // wait for all files to finish
	close(errCh)

	// Flush any remaining batch
	if err := p.flushBatch(); err != nil {
		return err
	}

	if len(errCh) > 0 {
		var allErrs []string
		for e := range errCh {
			allErrs = append(allErrs, e.Error())
		}
		return fmt.Errorf("zip processing completed with errors: %s", strings.Join(allErrs, "; "))
	}

	return nil
}

// ProcessZipEntry handles a single file entry in the zip
func (p *FileProcessServiceImpl) ProcessZipEntry(f *zip.File) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	name := strings.ToLower(f.Name)
	switch {
	case strings.HasSuffix(name, ".zip"):
		return p.ProcessNestedZip(rc, name)
	case strings.HasSuffix(name, ".csv") || strings.HasSuffix(name, ".mdff"):
		return p.ProcessCSVStream(rc, f.Name)
	default:
		return nil
	}
}

// processNestedZip writes nested zip to temp file and processes recursively
func (p *FileProcessServiceImpl) ProcessNestedZip(rc io.Reader, name string) error {

	tmpFile, err := os.CreateTemp("", "nested-*.zip")
	if err != nil {
		return err
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	if _, err := io.Copy(tmpFile, rc); err != nil {
		return err
	}

	return p.ProcessZipFile(tmpFile.Name())
}

func (p *FileProcessServiceImpl) ProcessCsvFile(filePath string) error {
	// Open CSV file directly
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open csv %s: %w", filePath, err)
	}
	defer f.Close()

	if err := p.ProcessCSVStream(f, filePath); err != nil {
		return err
	}

	return nil
}

// processCSVStream reads CSV and processes data in batches
func (p *FileProcessServiceImpl) ProcessCSVStream(r io.Reader, sourceName string) error {
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
				return p.flushBatch()
			}
			log.Logger.Error("csv read error", zap.Error(err))
			return fmt.Errorf("csv read: %w", err)
		}

		if len(fields) == 0 {
			continue
		}
		recordCount++
		if recordCount%p.opts.ProgressInterval == 0 {
			log.Logger.Info("CSV processing progress", zap.String("source", sourceName), zap.Int("records", recordCount))
		}
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
				log.Logger.Warn("invalid 200 record", zap.Int("line", recordCount), zap.Strings("fields", fields))
			}
		case "300":
			if currentNMI == "" || currentInterval == 0 {
				continue
			}
			if err := p.handle300(fields, currentNMI, currentInterval, sourceName, recordCount); err != nil {
				log.Logger.Error("failed to handle 300 record", zap.Int("line", recordCount), zap.Error(err))
				return err
			}
		case "900":
			if err := p.flushBatch(); err != nil {
				log.Logger.Error("flush batch failed at 900 record", zap.Error(err))
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
	numValues := min(availableValues, intervalsPerDay)

	for i := range numValues {
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
			return t, nil
		}
	}
	return time.Time{}, err
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
