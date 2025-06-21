package csv

import (
	"encoding/csv"
	"log"
	"os"
)

type ComparisonType string

// ComparisonType defines the type of comparison for filtering CSV data
const (
	Equal          ComparisonType = "="
	NonEqual       ComparisonType = "!="
	GreaterThan    ComparisonType = ">"
	GreaterOrEqual ComparisonType = ">="
	LessThan       ComparisonType = "<"
	LessOrEqual    ComparisonType = "<="
)

type Filter struct {
	column          string         // Column of CSV data
	comparisonType  ComparisonType // Type of comparison between the value in the column and the control value
	comparisonValue string         // Control value for comparison
	numeric         bool           // Whether to convert a value to a string. True for greater than and less than operations (including equal analogs). False for others.
}

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
