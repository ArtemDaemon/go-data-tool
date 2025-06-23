package csv

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"regexp"
	"strconv"
)

type comparisonType string

// ComparisonType defines the type of comparison for filtering CSV data
const (
	Equal          comparisonType = "="
	NonEqual       comparisonType = "!="
	GreaterThan    comparisonType = ">"
	GreaterOrEqual comparisonType = ">="
	LessThan       comparisonType = "<"
	LessOrEqual    comparisonType = "<="
)

var comparisonTypeFunctions = map[comparisonType]func(a, b string) (bool, error){
	Equal:    func(a, b string) (bool, error) { return a == b, nil },
	NonEqual: func(a, b string) (bool, error) { return a != b, nil },
	GreaterThan: func(a, b string) (bool, error) {
		aFloat, bFloat, err := convertComparisonValuesToFloat(a, b)
		if err != nil {
			return false, err
		}
		return aFloat > bFloat, nil
	},
	GreaterOrEqual: func(a, b string) (bool, error) {
		aFloat, bFloat, err := convertComparisonValuesToFloat(a, b)
		if err != nil {
			return false, err
		}
		return aFloat >= bFloat, nil
	},
	LessThan: func(a, b string) (bool, error) {
		aFloat, bFloat, err := convertComparisonValuesToFloat(a, b)
		if err != nil {
			return false, err
		}
		return aFloat < bFloat, nil
	},
	LessOrEqual: func(a, b string) (bool, error) {
		aFloat, bFloat, err := convertComparisonValuesToFloat(a, b)
		if err != nil {
			return false, err
		}
		return aFloat <= bFloat, nil
	},
}

type Filter struct {
	column          string         // Column of CSV data
	comparisonType  comparisonType // Type of comparison between the value in the column and the control value
	comparisonValue string         // Control value for comparison
}

func convertComparisonValuesToFloat(a, b string) (float64, float64, error) {
	aFloat, err := strconv.ParseFloat(a, 64)
	if err != nil {
		return 0, 0, errors.New("column value is not numeric")
	}
	bFloat, err := strconv.ParseFloat(b, 64)
	if err != nil {
		return 0, 0, errors.New("comparison value is not numeric")
	}
	return aFloat, bFloat, nil
}

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
