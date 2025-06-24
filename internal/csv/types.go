package csv

import (
	"errors"
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
