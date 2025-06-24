package csv

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"regexp"
)

func ParseCSV(filepath string, filters []Filter) ([][]string, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records [][]string
	csvReader := csv.NewReader(f)
	headers, err := csvReader.Read()
	if err != nil {
		return nil, err
	}
	records = append(records, headers)

	headerIndexes := make(map[string]int)
	for i, header := range headers {
		headerIndexes[header] = i
	}

	for {
		record, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		totalComparisonResult := true
		for _, filter := range filters {
			index, ok := headerIndexes[filter.column]
			if !ok {
				return nil, errors.New("filter for non-existent column")
			}
			columnValue := record[index]

			comparisonResult, err := comparisonTypeFunctions[filter.comparisonType](columnValue, filter.comparisonValue)
			if err != nil {
				return nil, err
			}
			if !comparisonResult {
				totalComparisonResult = false
				break
			}
		}

		if totalComparisonResult {
			records = append(records, record)
		}
	}

	return records, nil
}

func SaveCSV(records [][]string, filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.WriteAll(records)
	if err != nil {
		return err
	}
	return nil
}

func ParseFilter(filter string) (*Filter, error) {
	re := regexp.MustCompile(`(?i)([a-z0-9]+)(!=|>=|<=|=|>|<)([a-z0-9]+)`)
	filterParts := re.FindStringSubmatch(filter)

	// [Whole string, column, comparison type, comparison value]
	if len(filterParts) != 4 {
		return nil, errors.New("incorrect filter syntax")
	}

	operation, err := parseOperation(filterParts[2])
	if err != nil {
		return nil, err
	}

	return &Filter{filterParts[1], operation, filterParts[3]}, nil
}

func parseOperation(op string) (comparisonType, error) {
	switch op {
	case "=":
		return Equal, nil
	case "!=":
		return NonEqual, nil
	case ">":
		return GreaterThan, nil
	case ">=":
		return GreaterOrEqual, nil
	case "<":
		return LessThan, nil
	case "<=":
		return LessOrEqual, nil
	default:
		return "", errors.New("unknown operation")
	}
}
