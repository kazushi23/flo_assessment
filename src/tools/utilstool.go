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

// Normal NEM12 CSV
func GenerateNEM12Normal(fileName string, intervalMinutes, num200Records, max300Per200 int) error {
	return generateNEM12(fileName, intervalMinutes, num200Records, max300Per200, false)
}

// Malformed NEM12 CSV
func GenerateNEM12Malformed(fileName string, intervalMinutes, num200Records, max300Per200 int) error {
	return generateNEM12(fileName, intervalMinutes, num200Records, max300Per200, true)
}

// shared generator function
func generateNEM12(fileName string, intervalMinutes, num200Records, max300Per200 int, malform bool) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	startDate := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)
	nmiBase := "NEM12" + fmt.Sprintf("%02d", rand.Intn(1000))
	// nmiBase := "NEM12"

	// 100 header
	w.Write([]string{"100", "NEM12", startDate.Format("20060102150405"), "CNRGYMDP", "NEMMCO"})

	for i := range num200Records {
		nmi := fmt.Sprintf("%s%02d", nmiBase, i+1)
		registerID := fmt.Sprintf("E%d", rand.Intn(10)+1)

		// 200 record
		w.Write([]string{"200", nmi, "E1E2", registerID, registerID, fmt.Sprintf("N%d", i+1),
			"1002", "KWH", fmt.Sprintf("%d", intervalMinutes)})

		// multiple 300 interval rows with different dates
		num300Rows := rand.Intn(max300Per200) + 1
		for j := 0; j < num300Rows; j++ {
			date := startDate.AddDate(0, 0, j).Format("20060102")
			intervals := 24 * 60 / intervalMinutes
			values := make([]string, intervals)
			for k := 0; k < intervals; k++ {
				if malform && rand.Float32() < 0.05 {
					values[k] = "ERROR" // intentionally malformed
				} else {
					values[k] = fmt.Sprintf("%.2f", rand.Float64()*600)
				}
			}
			row := append([]string{"300", date}, values...)
			row = append(row, "A", "", "", startDate.Format("20060102150405"), startDate.Format("20060102150405"))
			w.Write(row)
		}

		// optional 500 record
		if rand.Float32() < 0.3 {
			if malform && rand.Float32() < 0.2 {
				w.Write([]string{"500", "O", "", "INVALID_DATE", "NOT_A_NUMBER"})
			} else {
				w.Write([]string{"500", "O", fmt.Sprintf("S%05d", i+1), startDate.Format("20060102150405"), fmt.Sprintf("%.2f", rand.Float64()*1000)})
			}
		}
	}

	// 900 trailer
	w.Write([]string{"900"})
	fmt.Printf("Generated NEM12 CSV: %s (malform=%v)\n", fileName, malform)
	return nil
}
