package service

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type ValidationError struct {
	Line    int
	Message string
}

// type NMIInterval struct {
// 	NMI            string
// 	IntervalDate   string
// 	FileCreateDate string // from 100 record, fields[2]
// }

type FileCheckerServiceImpl struct {
}

// // Check if NMI + IntervalDate exists in the database
// func (f *FileCheckerServiceImpl) NmiIntervalExists(nmi string, intervalDate string, fileCreationDate string, currentIntervalLength int) (bool, error) {
// 	// check if this combination is processed before with NEMFILEENTITY
// 	// nmi (300) - intervalDate (300) - currentIntervalLength (200)
// 	// if exist, check the filecreationDate
// 	// needsUpdate => so that i can trigger processrow later (only if this.filecreationdate > db.filecreationdate)
// 	// if needsUpdate == true => update redis or whereever i am keeping the combination of nmi-intervalDate-currentIntervalLength: fileCreationDate
// 	db := mysql.GetDB()
// 	var record entity.NemFileEntity

// 	parsedIntervalDate, err := ICsvProcessService.ParseIntervalDates(intervalDate)
// 	if err != nil {
// 		return false, fmt.Errorf("invalid interval date: %w", err)
// 	}

// 	parsedFileDate, err := ICsvProcessService.ParseIntervalDates(fileCreationDate)
// 	if err != nil {
// 		return false, fmt.Errorf("invalid file creation date: %w", err)
// 	}

// 	err = db.Where("nmi = ? AND interval_length = ? AND interval_date = ?", nmi, currentIntervalLength, parsedIntervalDate).
// 		First(&record).Error

// 	if err == gorm.ErrRecordNotFound {
// 		// No record, skip processing (row will not duplicate)
// 		return false, nil
// 	} else if err != nil {
// 		return false, err
// 	}

// 	// Row exists, check file creation date
// 	if parsedFileDate.After(record.FileCreationDate) {
// 		// Incoming file is newer, update record to trigger processing
// 		record.FileCreationDate = parsedFileDate
// 		if err := db.Save(&record).Error; err != nil {
// 			return false, err
// 		}
// 		return true, nil
// 	}

// 	// Row exists but file is older, skip processing
// 	return false, nil
// }
// func (f *FileCheckerServiceImpl) ProcessRow(row []string, currentIntervalLength int) error {
// 	// update the entire row into db

// 	return nil
// }

// CheckNEMFile validates a NEM12/13 file
// Validate file, any error reject as file integrity is wrong
// Flag this out to data owner, DO NOT PROCESS ANYTHING
func (f *FileCheckerServiceImpl) CheckNEMFile(path string) ([]ValidationError, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lineNum int
	var errors []ValidationError

	headerFound := false
	endFound := false
	nemVersion := ""
	// fileCreateDate := ""
	currentNMI := ""           // track NMI from 200 record
	currentIntervalLength := 0 // interval length from 200 record

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		fields := strings.Split(line, ",")

		if len(fields) == 0 || (len(fields) == 1 && fields[0] == "") {
			errors = append(errors, ValidationError{lineNum, "empty line"})
			continue
		}

		recordType := fields[0]
		switch recordType {
		case "100":
			if headerFound {
				errors = append(errors, ValidationError{lineNum, "multiple header records"})
			}
			headerFound = true
			if len(fields) < 5 {
				errors = append(errors, ValidationError{lineNum, "header must have at least 5 fields"})
			}
			nemVersion = fields[1]
			// fileCreateDate = fields[2] // store file creation datetime
			if nemVersion != "NEM12" && nemVersion != "NEM13" {
				errors = append(errors, ValidationError{lineNum, "unsupported version: " + nemVersion})
			}

		case "900":
			if endFound {
				errors = append(errors, ValidationError{lineNum, "multiple end records"})
			}
			endFound = true
			if !headerFound {
				errors = append(errors, ValidationError{lineNum, "end record found before header"})
			}

		case "200":
			if len(fields) < 10 {
				errors = append(errors, ValidationError{lineNum, "200 record too short"})
				currentNMI = ""
				currentIntervalLength = 0
			} else {
				currentNMI = fields[1] // track NMI for following 300 records
				if len(currentNMI) > 10 {
					errors = append(errors, ValidationError{lineNum, fmt.Sprintf("NMI too long: %s", currentNMI)})
				}
				var err error
				currentIntervalLength, err = strconv.Atoi(fields[8])
				if err != nil {
					errors = append(errors, ValidationError{lineNum, "invalid interval length in 200 record"})
					currentIntervalLength = 0
				}
			}

		case "300":
			if currentNMI == "" {
				errors = append(errors, ValidationError{lineNum, "no preceding 200 record to provide NMI"})
				continue
			}
			if currentIntervalLength <= 0 {
				errors = append(errors, ValidationError{lineNum, "invalid or missing interval length from 200 record"})
				continue
			}

			dateStr := strings.TrimSpace(fields[1])
			_, err := ICsvProcessService.ParseIntervalDates(dateStr)
			if err != nil {
				errors = append(errors, ValidationError{
					Line:    lineNum,
					Message: err.Error(),
				})
			}
			// Validate interval values count
			expectedValues := 1440 / currentIntervalLength
			actualValues := len(fields) - 7 // skip RecordIndicator,IntervalDate,QualityMethod,ReasonCode,ReasonDescription,UpdateDateTime,MSATSLoadDateTime
			// Too few values
			if actualValues < expectedValues {
				errors = append(errors, ValidationError{
					Line:    lineNum,
					Message: fmt.Sprintf("not enough interval values: expected %d got %d", expectedValues, actualValues),
				})
			}

			// Too many values
			if actualValues > expectedValues {
				errors = append(errors, ValidationError{
					Line:    lineNum,
					Message: fmt.Sprintf("too many interval values: expected %d got %d", expectedValues, actualValues),
				})
			}

			// Attempt to parse values that exist
			for i := 0; i < min(expectedValues, actualValues); i++ {
				raw := strings.TrimSpace(fields[2+i])
				if raw == "" {
					continue // allow empty -> handled later
				}
				if _, err := strconv.ParseFloat(strings.ReplaceAll(raw, ",", ""), 64); err != nil {
					errors = append(errors, ValidationError{
						Line:    lineNum,
						Message: fmt.Sprintf("invalid float at interval %d: %v", i+1, err),
					})
				}
			}

		case "400":
			// add 400 validation if needed

		case "500", "550", "250":
			// add other record validation if needed

		default:
			errors = append(errors, ValidationError{lineNum, "unknown record type: " + recordType})
		}
	}

	if !headerFound {
		errors = append(errors, ValidationError{0, "header record not found"})
	}
	if !endFound {
		errors = append(errors, ValidationError{0, "end record not found"})
	}

	return errors, scanner.Err()
}
