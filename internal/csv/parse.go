package csv

import (
	"encoding/csv"
	"log"
	"os"
)

func ParseCSV(filepath string) [][]string {
	f, err := os.Open(filepath)
	if err != nil {
		log.Fatal("Unable to read input file", err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parese file as CSV", err)
	}
	return records
}

func SaveCSV(records [][]string, filepath string) {
	file, err := os.Create(filepath)
	if err != nil {
		log.Fatal("Cannot create file", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.WriteAll(records)
	if err != nil {
		log.Fatal("Cannot write to file", err)
	}
}
