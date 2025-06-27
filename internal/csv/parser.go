package csv

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strconv"
)

func ParseCSVStructure(filepath string) (Scheme, error) {
	scheme := Scheme{}
	f, err := os.Open(filepath)
	if err != nil {
		return scheme, err
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	headers, err := csvReader.Read()
	if err != nil {
		return scheme, err
	}

	columns := make(map[string]ColumnInfo)
	for i, header := range headers {
		columns[header] = ColumnInfo{
			Index:      i,
			ColumnType: TypeString,
		}
	}
	scheme.Headers = headers

	var intColumnIndices, floatColumnIndices []int
	firstRow, err := csvReader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			scheme.Columns = columns
			return scheme, nil
		}
		return scheme, err
	}

	for i, column := range firstRow {
		columnType := getTypeByValue(column)

		switch columnType.Name() {
		case TypeInt.TypeName:
			intColumnIndices = append(intColumnIndices, i)
		case TypeFloat.TypeName:
			floatColumnIndices = append(floatColumnIndices, i)
		}
	}

	for len(intColumnIndices) != 0 || len(floatColumnIndices) != 0 {
		record, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return scheme, err
		}

		var newIntIndices []int
		for _, columnIndex := range intColumnIndices {
			if getTypeByValue(record[columnIndex]).Name() == TypeInt.TypeName {
				newIntIndices = append(newIntIndices, columnIndex)
			}
		}
		intColumnIndices = newIntIndices

		var newFloatIndices []int
		for _, columnIndex := range floatColumnIndices {
			if getTypeByValue(record[columnIndex]).Name() == TypeFloat.TypeName {
				newFloatIndices = append(newFloatIndices, columnIndex)
			}
		}
		floatColumnIndices = newFloatIndices
	}

	for _, columnIndex := range intColumnIndices {
		header := headers[columnIndex]
		colInfo := columns[header]
		colInfo.ColumnType = TypeInt
		columns[header] = colInfo
	}
	for _, columnIndex := range floatColumnIndices {
		header := headers[columnIndex]
		colInfo := columns[header]
		colInfo.ColumnType = TypeFloat
		columns[header] = colInfo
	}

	scheme.Columns = columns
	return scheme, nil
}

func ParseCSV(filepath string, scheme Scheme, filters []Filter, aggregations []Aggregation) ([][]string, error) {
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

	columns := scheme.Columns

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
			column := columns[filter.column]
			columnValue := record[column.Index]

			comparisonResult, err := column.ColumnType.Compare(columnValue, filter.comparisonValue, filter.comparisonType)
			if err != nil {
				return nil, err
			}
			if !comparisonResult {
				totalComparisonResult = false
				break
			}
		}

		if !totalComparisonResult {
			continue
		}

		records = append(records, record)
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

func ParseFilter(filter string, scheme Scheme) (Filter, error) {
	filterObj := Filter{}

	re := regexp.MustCompile(`(?i)([a-z0-9]+)(!=|>=|<=|=|>|<)([a-z0-9]+)`)
	filterParts := re.FindStringSubmatch(filter)

	// [Whole string, column, comparison type, comparison value]
	if len(filterParts) != 4 {
		return filterObj, errors.New("incorrect filter syntax")
	}

	// Check if column exists
	column, ok := scheme.Columns[filterParts[1]]
	if !ok {
		return filterObj, errors.New("filter for non-existent column")
	}

	operation, err := parseOperation(filterParts[2])
	if err != nil {
		return filterObj, err
	}

	// Check if non numeric value pass to the numeric type column
	valueType := getTypeByValue(filterParts[3])
	if column.ColumnType != TypeString && valueType != column.ColumnType {
		return filterObj, fmt.Errorf("value type is '%s', column type is '%s'", valueType.Name(), column.ColumnType.Name())
	}

	filterObj.column = filterParts[1]
	filterObj.comparisonType = operation
	filterObj.comparisonValue = filterParts[3]

	return filterObj, nil
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
		return 0, errors.New("unknown operation")
	}
}

func getTypeByValue(value string) ColumnTypeInterface {
	if _, err := strconv.Atoi(value); err == nil {
		return TypeInt
	}

	f, err := strconv.ParseFloat(value, 64)
	if err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
		return TypeFloat
	}

	return TypeString
}

func ParseAggregation(aggregationColumn string, aggraggregationType AggregationType, scheme Scheme) (Aggregation, error) {
	aggregation := Aggregation{}

	// Check if column exists
	columns := scheme.Columns
	column, ok := columns[aggregationColumn]
	if !ok {
		return aggregation, errors.New("filter for non-existent column")
	}

	// For sum and avg check if columnType is not string
	if (aggraggregationType == Sum || aggraggregationType == Avg) && column.ColumnType == TypeString {
		var typeStr string
		if aggraggregationType == Sum {
			typeStr = "sum"
		} else {
			typeStr = "avg"
		}
		return aggregation, fmt.Errorf("'%s' aggregation type can only be applied to columns with numeric data type", typeStr)
	}

	aggregation.aggregationType = aggraggregationType
	aggregation.column = aggregationColumn
	return aggregation, nil
}
