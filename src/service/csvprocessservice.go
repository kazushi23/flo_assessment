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
			if err := p.handle300(fields, currentNMI, currentInterval, sourceName, recordCount, fps); err != nil {
				log.Logger.Error("failed to handle 300 record", zap.Int("line", recordCount), zap.Error(err))
				return err
			}
		case "900":
			if err := fps.flushBatch(); err != nil {
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
func (p *CsvProcessServiceImpl) handle200(fields []string, source string, record int) (string, int, bool) {
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
func (p *CsvProcessServiceImpl) handle300(fields []string, currentNMI string, currentInterval int, source string, record int, fps *FileProcessServiceImpl) error {
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
		if err := fps.checkContextCancelled(); err != nil {
			return err
		}

		raw := strings.TrimSpace(fields[2+i])
		if raw == "" && !fps.opts.AllowEmptyReading {
			continue
		} else if raw == "" {
			raw = "0"
		}

		val, err := strconv.ParseFloat(strings.ReplaceAll(raw, ",", ""), 64)
		if err != nil {
			continue
		}

		ts := intervalDate.Add(time.Duration(i*currentInterval) * time.Minute)

		fps.addToBatch(entity.MeterReadingsEntity{
			Id:          tools.NewUuid(),
			Nmi:         currentNMI,
			Timestamp:   ts,
			Consumption: val,
		})
	}
	return nil
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
