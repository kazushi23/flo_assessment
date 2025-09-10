package service

import (
	"archive/zip"
	"context"
	"flo/assessment/config/log"
	"flo/assessment/entity"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"go.uber.org/zap"
)

type FileProcessServiceImpl struct {
	ctx         context.Context
	batch       []entity.MeterReadingsEntity
	batchMutex  sync.Mutex     // protects access to batch
	wg          sync.WaitGroup // wait for all goroutines to finish
	concurrency int            // max concurrent workers
}

// NewFileProcessServiceImpl creates a service with a concurrency limit
func NewFileProcessServiceImpl(ctx context.Context, concurrency int) *FileProcessServiceImpl {
	return &FileProcessServiceImpl{
		ctx:         ctx,
		concurrency: concurrency,
	}
}

// ProcessZipFile reads a zip file and processes its entries concurrently
func (p *FileProcessServiceImpl) ProcessZipFile(zipPath string) error {
	log.Logger.Info("Processing zip file start", zap.String("zipPath", zipPath))

	r, err := zip.OpenReader(zipPath)
	if err != nil {
		log.Logger.Error("failed to open zip", zap.String("zipPath", zipPath), zap.Error(err))
		return fmt.Errorf("open zip %s: %w", zipPath, err)
	}
	defer r.Close()

	// Semaphore channel to limit concurrency
	sem := make(chan struct{}, p.concurrency)

	for _, f := range r.File {
		// Respect context cancellation
		if p.ctx.Err() != nil {
			log.Logger.Error("context cancelled, stopping zip processing", zap.String("zipPath", zipPath), zap.Error(p.ctx.Err()))
			return p.ctx.Err()
		}

		// Skip directories, only process files
		if f.FileInfo().IsDir() {
			continue
		}

		// Acquire a slot in the semaphore
		sem <- struct{}{}
		p.wg.Add(1)

		go func(f *zip.File) {
			defer p.wg.Done()
			defer func() { <-sem }() // release semaphore slot

			if err := p.ProcessZipEntry(f); err != nil {
				log.Logger.Error("failed to process zip entry", zap.String("entry", f.Name), zap.Error(err))
			}
		}(f)
	}

	// Wait for all goroutines to finish
	p.wg.Wait()

	// Final flush for any remaining batch
	if err := p.flushBatch(); err != nil {
		log.Logger.Error("flush final batch after zip", zap.Error(err))
		return err
	}

	return nil
}

// ProcessZipEntry handles a single file in the zip
func (p *FileProcessServiceImpl) ProcessZipEntry(f *zip.File) error {
	name := strings.ToLower(f.Name)
	rc, err := f.Open()
	if err != nil {
		log.Logger.Warn("failed to open zip entry", zap.String("zipEntry", name), zap.Error(err))
		return err
	}
	defer rc.Close()

	switch {
	case strings.HasSuffix(name, ".zip"):
		// Nested zip: write to temp file and process recursively
		return p.processNestedZip(rc, name)

	case strings.HasSuffix(name, ".csv"):
		log.Logger.Info("processing CSV entry", zap.String("entry", f.Name))
		// Directly process the csv and store in db
		return p.processCSVStream(rc, f.Name)

	default:
		log.Logger.Info("skipping unsupported file type", zap.String("entry", f.Name))
	}

	return nil
}

// processNestedZip writes the zip contents to a temp file and processes it recursively
func (p *FileProcessServiceImpl) processNestedZip(rc io.Reader, name string) error {
	tmpFile, err := os.CreateTemp("", "nested-*.zip")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name()) // remove temp file
	defer tmpFile.Close()           // close temp file

	if _, err := io.Copy(tmpFile, rc); err != nil {
		return err
	}

	log.Logger.Info("processing nested zip", zap.String("nestedZip", name))
	return p.ProcessZipFile(tmpFile.Name())
}

// processCSVStream reads a CSV stream and processes data in batches
func (p *FileProcessServiceImpl) processCSVStream(r io.Reader, sourceName string) error {
	// TODO: parse CSV rows, append to p.batch safely
	// Example: p.addToBatch(entity)
	return nil
}

// addToBatch safely appends a row to the batch
func (p *FileProcessServiceImpl) addToBatch(row entity.MeterReadingsEntity) {
	p.batchMutex.Lock()
	defer p.batchMutex.Unlock()

	p.batch = append(p.batch, row)

	// Flush batch if size reaches threshold (e.g., 1000)
	if len(p.batch) >= 1000 {
		if err := p.flushBatch(); err != nil {
			log.Logger.Error("flush batch failed", zap.Error(err))
		}
	}
}

// flushBatch inserts batch into database safely
func (p *FileProcessServiceImpl) flushBatch() error {
	p.batchMutex.Lock()
	defer p.batchMutex.Unlock()

	if len(p.batch) == 0 {
		return nil
	}

	// TODO: insert p.batch into database here
	// Example: mysql.Save(p.batch)

	p.batch = []entity.MeterReadingsEntity{} // reset batch
	return nil
}
