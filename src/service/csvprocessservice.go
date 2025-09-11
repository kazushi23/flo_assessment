package service

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flo/assessment/config/log"
	"flo/assessment/entity"
	"flo/assessment/src/tools"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

type CsvProcessServiceImpl struct{}

func (p *CsvProcessServiceImpl) ProcessCsvFile(filePath string, fps *FileProcessServiceImpl) error {
	// Open CSV file directly
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open csv %s: %w", filePath, err)
	}
	defer f.Close()

	if err := p.ProcessCSVStream(f, filePath, fps); err != nil {
		return err
	}

	return nil
}

// processCSVStream reads CSV and processes data in batches
func (p *CsvProcessServiceImpl) ProcessCSVStream(r io.Reader, sourceName string, fps *FileProcessServiceImpl) error {
	csvr := csv.NewReader(bufio.NewReader(r))
	csvr.TrimLeadingSpace = true
	csvr.LazyQuotes = true
	csvr.FieldsPerRecord = -1

	var currentNMI string
	var currentInterval int
	var recordCount int

	for {
		if err := fps.checkContextCancelled(); err != nil {
			return err
		}

		fields, err := csvr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return fps.flushBatch()
			}
			log.Logger.Error("csv read error", zap.Error(err))
			return fmt.Errorf("csv read: %w", err)
		}

		if len(fields) == 0 {
			continue
		}
		recordCount++
		if recordCount%fps.opts.ProgressInterval == 0 {
			log.Logger.Info("CSV processing progress", zap.String("source", sourceName), zap.Int("records", recordCount))
		}
		recordIndicator := strings.TrimSpace(fields[0])

		switch recordIndicator {
		case "200":
			nmi, interval := p.handle200(fields, sourceName, recordCount, fps)
			currentNMI = nmi
			currentInterval = interval
		case "300":
			if currentNMI != "" || currentInterval != 0 {
				p.handle300(fields, currentNMI, currentInterval, sourceName, recordCount, fps)
			}
		case "900":
			fps.flushBatch()
			return nil
		default:
			log.Logger.Warn("ignored record type", zap.Int("line", recordCount), zap.Strings("fields", fields))
			fps.addErrorRowCSV(fields, fmt.Errorf("ignored record type: %s", recordIndicator))
		}
	}

}

// handle200 parses a 200 record
func (p *CsvProcessServiceImpl) handle200(fields []string, source string, record int, fps *FileProcessServiceImpl) (string, int) {
	if len(fields) <= 8 {
		err := fmt.Errorf("not enough fields in 200 record")
		log.Logger.Warn("malformed 200 record", zap.Int("line", record), zap.Strings("fields", fields), zap.Error(err))
		fps.addErrorRowCSV(fields, err)
		return "", 0
	}
	nmi := strings.TrimSpace(fields[1])
	ivalStr := strings.TrimSpace(fields[8])
	if ivalStr == "" {
		err := fmt.Errorf("interval value empty")
		log.Logger.Warn("malformed 200 record", zap.Int("line", record), zap.Strings("fields", fields), zap.Error(err))
		fps.addErrorRowCSV(fields, err)
		return nmi, 0
	}
	iv, err := strconv.Atoi(ivalStr)
	if err != nil {
		log.Logger.Warn("invalid interval value", zap.Int("line", record), zap.Strings("fields", fields), zap.Error(err))
		fps.addErrorRowCSV(fields, err)
		return nmi, 0
	}
	return nmi, iv
}

// handle300 parses 300 record and adds data to batch
func (p *CsvProcessServiceImpl) handle300(fields []string, currentNMI string, currentInterval int, source string, record int, fps *FileProcessServiceImpl) {
	if len(fields) < 3 {
		err := fmt.Errorf("not enough fields in 300 record")
		log.Logger.Warn("malformed 300 record", zap.Int("line", record), zap.Strings("fields", fields), zap.Error(err))
		fps.addErrorRowCSV(fields, err)
		return
	}

	dateStr := strings.TrimSpace(fields[1])
	intervalDate, err := p.ParseIntervalDates(dateStr)
	if err != nil {
		log.Logger.Warn("invalid date format in 300 record", zap.Int("line", record), zap.Strings("fields", fields), zap.Error(err))
		fps.addErrorRowCSV(fields, err)
		return
	}

	intervalsPerDay := 1440 / currentInterval
	availableValues := len(fields) - 2
	numValues := min(availableValues, intervalsPerDay)

	for i := range numValues {
		if err := fps.checkContextCancelled(); err != nil {
			log.Logger.Warn("context cancelled during CSV processing", zap.Error(err))
			return
		}

		raw := strings.TrimSpace(fields[2+i])
		if raw == "" && !fps.opts.AllowEmptyReading {
			err := fmt.Errorf("empty value not allowed at interval %d", i)
			log.Logger.Warn("empty value in 300 record", zap.String("nmi", currentNMI), zap.Error(err))
			fps.addErrorRowCSV(fields, err)
			continue
		} else if raw == "" {
			raw = "0"
		}

		val, err := strconv.ParseFloat(strings.ReplaceAll(raw, ",", ""), 64)
		if err != nil {
			log.Logger.Warn("invalid consumption value", zap.String("nmi", currentNMI), zap.String("raw_value", raw), zap.Error(err))
			fps.addErrorRowCSV(fields, err)
			continue
		}

		ts := intervalDate.Add(time.Duration(i*currentInterval) * time.Minute)

		fps.addToBatch(entity.MeterReadingsEntity{
			Id:          tools.NewUuid(),
			Nmi:         currentNMI,
			Timestamp:   ts,
			Consumption: val,
		})

		if len(fps.batch) >= fps.opts.BatchSize {
			if err := fps.flushBatch(); err != nil {
				log.Logger.Error("failed to flush batch mid-file", zap.Error(err))
			}
		}
	}
}

// ParseIntervalDates parses multiple date formats
func (p *CsvProcessServiceImpl) ParseIntervalDates(s string) (time.Time, error) {
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
