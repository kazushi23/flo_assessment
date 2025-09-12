package service

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flo/assessment/entity"
	"flo/assessment/src/tools"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type CsvProcessServiceImpl struct{}

// ProcessCsvFileToChannel parses a CSV and sends rows to channel
func (p *CsvProcessServiceImpl) ProcessCsvFileToChannel(filePath string, fps *FileProcessServiceImpl, out chan<- entity.MeterReadingsEntity) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open csv %s: %w", filePath, err)
	}
	defer f.Close()

	csvr := csv.NewReader(bufio.NewReader(f))
	csvr.TrimLeadingSpace = true
	csvr.LazyQuotes = true
	csvr.FieldsPerRecord = -1

	var currentNMI string
	var currentInterval int
	var recordCount int
	var fileCreationDate string
	for {
		if err := fps.checkContextCancelled(); err != nil {
			return err
		}

		fields, err := csvr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("csv read: %w", err)
		}
		if len(fields) == 0 {
			continue
		}

		recordCount++
		recordIndicator := strings.TrimSpace(fields[0])

		switch recordIndicator {
		case "100":
			// 			100,NEM12,200301011534,MDP1,Retailer1
			// RecordIndicator,VersionHeader,DateTime,FromParticipant,ToParticipant
			fileCreationDate = fields[2]
		case "200":
			nmi, interval := p.handle200(fields, recordCount, fps)
			currentNMI = nmi
			currentInterval = interval
		case "300":
			if currentNMI != "" || currentInterval != 0 {
				p.handle300ToChannel(fields, currentNMI, currentInterval, fps, out, fileCreationDate)
			}
		case "900":
			return nil
		}
	}
}

// data here should be clean already due to pre-processing
func (p *CsvProcessServiceImpl) handle300ToChannel(
	fields []string,
	currentNMI string,
	currentInterval int,
	fps *FileProcessServiceImpl,
	out chan<- entity.MeterReadingsEntity,
	fileCreationDateStr string,
) {
	if len(fields) < 3 {
		fps.addErrorRowCSV(fields, fmt.Errorf("not enough fields in 300 record"))
		return
	}

	dateStr := strings.TrimSpace(fields[1])
	intervalDate, err := p.ParseIntervalDates(dateStr)
	if err != nil {
		fps.addErrorRowCSV(fields, err)
		return
	}

	fileCreationDate, err := p.ParseIntervalDates(fileCreationDateStr)
	if err != nil {
		fps.addErrorRowCSV(fields, err)
		return
	}

	intervalsPerDay := 1440 / currentInterval
	availableValues := len(fields) - 2
	numValues := min(availableValues, intervalsPerDay)

	for i := range numValues {
		raw := strings.TrimSpace(fields[2+i])
		if raw == "" && !fps.opts.AllowEmptyReading {
			fps.addErrorRowCSV(fields, fmt.Errorf("empty value not allowed at interval %d", i))
			continue
		} else if raw == "" {
			raw = "0"
		}

		val, err := strconv.ParseFloat(strings.ReplaceAll(raw, ",", ""), 64)
		if err != nil {
			fps.addErrorRowCSV(fields, err)
			continue
		}

		ts := intervalDate.Add(time.Duration(i*currentInterval) * time.Minute)

		out <- entity.MeterReadingsEntity{
			Id:               tools.NewUuid(),
			Nmi:              currentNMI,
			Timestamp:        ts,
			Consumption:      val,
			FileCreationDate: fileCreationDate,
		}
	}
}

// handle200 parses a 200 record
// AT THIS STAGE, DATA IS CLEAN
func (p *CsvProcessServiceImpl) handle200(fields []string, record int, fps *FileProcessServiceImpl) (string, int) {
	// if len(fields) <= 8 {
	// 	err := fmt.Errorf("not enough fields in 200 record")
	// 	log.Logger.Warn("malformed 200 record", zap.Int("line", record), zap.Strings("fields", fields), zap.Error(err))
	// 	fps.addErrorRowCSV(fields, err)
	// 	return "", 0
	// }
	// nmi := strings.TrimSpace(fields[1])
	// if len(nmi) > 10 {
	// 	err := fmt.Errorf("NMI too long")
	// 	log.Logger.Warn("NMI too long", zap.Int("line", record), zap.Strings("fields", fields), zap.Error(err))
	// 	fps.addErrorRowCSV(fields, err)
	// 	return "", 0
	// }
	// ivalStr := strings.TrimSpace(fields[8])
	// if ivalStr == "" {
	// 	err := fmt.Errorf("interval value empty")
	// 	log.Logger.Warn("malformed 200 record", zap.Int("line", record), zap.Strings("fields", fields), zap.Error(err))
	// 	fps.addErrorRowCSV(fields, err)
	// 	return "", 0
	// }
	// iv, err := strconv.Atoi(ivalStr)
	// if err != nil {
	// 	log.Logger.Warn("invalid interval value", zap.Int("line", record), zap.Strings("fields", fields), zap.Error(err))
	// 	fps.addErrorRowCSV(fields, err)
	// 	return "", 0
	// }
	nmi := strings.TrimSpace(fields[1])
	iv, _ := strconv.Atoi(strings.TrimSpace(fields[8]))

	return nmi, iv
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
