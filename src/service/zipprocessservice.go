package service

import (
	"archive/zip"
	"flo/assessment/config/log"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// ZipProcessServiceImpl manages NEM12 processing
type ZipProcessServiceImpl struct {
}

// ProcessZipFile reads a zip and processes all entries concurrently
func (p *ZipProcessServiceImpl) ProcessZipFile(filePath string, fps *FileProcessServiceImpl) error {
	r, err := zip.OpenReader(filePath)
	if err != nil {
		return fmt.Errorf("open zip %s: %w", filePath, err)
	}
	defer r.Close()

	errCh := make(chan error, len(r.File))
	var wg sync.WaitGroup

	for _, f := range r.File {
		if fps.checkContextCancelled() != nil {
			return fps.ctx.Err()
		}

		if f.FileInfo().IsDir() {
			continue
		}

		fps.sem <- struct{}{}
		wg.Add(1)
		go func(f *zip.File) {
			defer wg.Done()
			defer func() { <-fps.sem }()
			// entry point for zip file
			if err := p.ProcessZipEntry(f, fps); err != nil {
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
func (p *ZipProcessServiceImpl) ProcessZipEntry(f *zip.File, fps *FileProcessServiceImpl) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	name := strings.ToLower(f.Name)
	switch {
	case strings.HasSuffix(name, ".zip"):
		return p.ProcessNestedZip(rc, name, fps) // re-unzip again if current file is zip
	case strings.HasSuffix(name, ".csv") || strings.HasSuffix(name, ".mdff"):
		return IFileProcessService.RunPipeline(f.Name) // throw into csv processing pipeline
	default:
		return nil
	}
}

// processNestedZip writes nested zip to temp file and processes recursively
func (p *ZipProcessServiceImpl) ProcessNestedZip(rc io.Reader, name string, fps *FileProcessServiceImpl) error {

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

	return p.ProcessZipFile(tmpFile.Name(), fps)
}
