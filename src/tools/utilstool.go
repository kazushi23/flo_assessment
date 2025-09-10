package tools

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"time"

	uuid "github.com/satori/go.uuid"
)

func NewUuid() string {
	id := uuid.NewV4()
	return id.String()
}

// GenerateNEM12CSV generates a valid NEM12 CSV file
func GenerateNEM12CSV(fileName string, intervalMinutes int, numDays int, numRegisters int) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	startDate := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)
	nmiBase := "NEM12" + fmt.Sprintf("%03d", rand.Intn(1000)) // random NMI base

	// 100 - File Header
	w.Write([]string{"100", "NEM12", startDate.Format("20060102150405"), "CNRGYMDP", "NEMMCO"})

	for day := range numDays {
		date := startDate.AddDate(0, 0, day).Format("20060102")

		for reg := 1; reg <= numRegisters; reg++ {
			registerID := fmt.Sprintf("E%d", reg)
			nmi := fmt.Sprintf("%s%02d", nmiBase, reg)

			// 200 - NMI + Register
			w.Write([]string{
				"200", nmi, "E1E2", registerID, registerID, fmt.Sprintf("N%d", reg),
				"1002", "KWH", fmt.Sprintf("%d", intervalMinutes),
			})

			// 300 - Interval Data
			intervals := 24 * 60 / intervalMinutes
			values := make([]string, intervals)
			for i := range intervals {
				values[i] = fmt.Sprintf("%.2f", rand.Float64()*600)
			}

			row := append([]string{"300", date}, values...)
			row = append(row, "A", "", "", startDate.Format("20060102150405"), startDate.Format("20060102150405"))
			w.Write(row)

			// 500 - Optional B2B Record
			if rand.Float32() < 0.3 { // ~30% chance to add
				w.Write([]string{"500", "O", fmt.Sprintf("S%05d", reg), startDate.Format("20060102150405"), fmt.Sprintf("%.2f", rand.Float64()*1000)})
			}
		}
	}

	// 900 - File Trailer
	w.Write([]string{"900"})

	fmt.Printf("Generated NEM12 CSV: %s\n", fileName)
	return nil
}
