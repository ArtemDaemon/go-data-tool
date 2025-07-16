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
	// Declare scheme
	scheme := Scheme{}

	// Open the CSV file and check that the file exists
	f, err := os.Open(filepath)
	if err != nil {
		return scheme, err
	}
	defer f.Close()

	// Creating a reader
	csvReader := csv.NewReader(f)

	// Read the headers separately
	headers, err := csvReader.Read()
	if err != nil {
		return scheme, err
	}

	/*
		Create a map for storing columns info
		{
			"columnName": {
				Index: int,
				ColumnType: ColumnType
			}
		}
	*/
	columns := make(map[string]ColumnInfo)

	// All are set to type string in case there are no data rows in the file
	for i, header := range headers {
		columns[header] = ColumnInfo{
			Index:      i,
			ColumnType: TypeString,
		}
	}
	scheme.Headers = headers

	// Lists of indexes of columns with numeric values
	var intColumnIndices, floatColumnIndices []int

	/*
		Read the first line of data separately to
		check the types of each value and find the numeric types
	*/
	firstRow, err := csvReader.Read()
	if err != nil {
		// The error may mean that there is no more data to read
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

	/*
		If numeric columns are found, the file is read to the end to
		ensure that the column contains only numeric values
	*/
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

func ParseCSV(filepath string, scheme Scheme, filters []Filter, aggregations []Aggregator, groups []string) ([][]string, error) {
	// Open the CSV file and check that the file exists
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Final data
	var records [][]string
	// Creating a reader
	csvReader := csv.NewReader(f)
	// Array of column names
	var headers []string

	// Aggregation indication flag
	hasAggregations := len(aggregations) != 0
	// Grouping indication flag
	hasGroups := len(groups) != 0
	// Map for the unique aggregation columns
	aggragatedColumns := make(map[string]bool)
	/*
		Map for grouping aggregated values
		{
			"groupKey": {
				"columnName": ["1", "2", "3"...]
			}
		}
	*/
	groupingMeasuresMap := make(map[string]map[string][]string)
	/*
		Map for storing group fields
		{
			"groupKey": ["1", "2", "3"...]
		}
	*/
	groupingAttributesMap := make(map[string][]string)
	// If columns are specified for grouping, add grouping columns to the header
	if hasGroups {
		headers = append(headers, groups...)
	}
	/*
		If aggregations are specified,
		only the aggregation columns are added to the final headers
	*/
	if hasAggregations {
		for _, v := range aggregations {
			headers = append(headers, v.Name())
			aggragatedColumns[v.Column()] = true
		}
	}

	if !hasAggregations && !hasGroups {
		// If neither aggregation nor grouping is specified, add headers from the file
		headers, err = csvReader.Read()
		if err != nil {
			return nil, err
		}
	} else {
		// Otherwise, we deliberately skip the line with the headers from the file
		_, err = csvReader.Read()
		if err != nil {
			return nil, err
		}
	}
	records = append(records, headers)

	// Column Information Map
	columns := scheme.Columns

	for {
		// Reading lines from a file
		record, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		// Filtering result
		totalComparisonResult := true
		// Checking a row against all filters
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

		// Key for grouping
		groupKey := ""
		/*
			If grouping columns are specified,
			generate key and insert data
		*/
		if hasGroups {
			groupKey = generateGroupKey(record, groups, scheme)
			if _, ok := groupingAttributesMap[groupKey]; !ok {
				groupAttributes := make([]string, len(groups))
				for i, v := range groups {
					groupAttributes[i] = record[scheme.Columns[v].Index]
				}
				groupingAttributesMap[groupKey] = groupAttributes
			}
		}
		/*
			If aggregations are specified,
			save data for grouping
		*/
		if hasAggregations {
			if !hasGroups {
				groupKey = ""
			}
			groupingColumns, ok := groupingMeasuresMap[groupKey]
			if !ok {
				groupingColumns = make(map[string][]string)
			}
			for columnName := range aggragatedColumns {
				groupingValues := groupingColumns[columnName]
				groupingValues = append(groupingValues, record[scheme.Columns[columnName].Index])
				groupingColumns[columnName] = groupingValues
			}
			groupingMeasuresMap[groupKey] = groupingColumns
		}

		if !hasAggregations && !hasGroups {
			// Otherwise, save all data
			records = append(records, record)
		}
	}

	if hasAggregations || hasGroups {
		for key, attributes := range groupingAttributesMap {
			currentRecord := make([]string, len(aggregations)+len(groups))
			copy(currentRecord, attributes)
			for i, aggregation := range aggregations {
				aggregationResult, err := aggregation.Aggregate(groupingMeasuresMap[key][aggregation.Column()])
				if err != nil {
					return nil, err
				}
				currentRecord[i+len(groups)] = aggregationResult
			}
			records = append(records, currentRecord)
		}
	}

	return records, nil
}

func generateGroupKey(record []string, groups []string, scheme Scheme) string {
	var key string
	for _, v := range groups {
		key += record[scheme.Columns[v].Index]
		key += "_"
	}
	return key
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

func ParseAggregation(aggregationColumn string, aggregationType AggregationType, scheme Scheme) (Aggregator, error) {
	// Check if column exists
	columns := scheme.Columns
	column, ok := columns[aggregationColumn]
	if !ok {
		return nil, errors.New("filter for non-existent column")
	}

	// For sum and avg check if columnType is not string
	if (aggregationType == AggSum || aggregationType == AggAvg) && column.ColumnType == TypeString {
		return nil, fmt.Errorf("'%s' aggregation type can only be applied to columns with numeric data type", aggregationType)
	}

	columnType := column.ColumnType

	switch aggregationType {
	case AggSum:
		switch columnType {
		case TypeInt:
			return SumAggregator[int]{aggregationColumn, TypeInt}, nil
		case TypeFloat:
			return SumAggregator[float64]{aggregationColumn, TypeFloat}, nil
		default:
			return nil, fmt.Errorf("cannot aggregate type %s", column.ColumnType.Name())
		}
	case AggAvg:
		switch column.ColumnType.Name() {
		case TypeInt.TypeName:
			return AvgAggregator[int]{columnName: aggregationColumn, columnType: TypeInt}, nil
		case TypeFloat.TypeName:
			return AvgAggregator[float64]{columnName: aggregationColumn, columnType: TypeFloat}, nil
		default:
			return nil, fmt.Errorf("cannot aggregate type %s", column.ColumnType.Name())
		}
	case AggCount:
		return CountAggregator[string]{columnName: aggregationColumn}, nil
	case AggCountDistinct:
		return CountDistinctAggregator[string]{columnName: aggregationColumn}, nil
	case AggMax:
		switch column.ColumnType.Name() {
		case TypeInt.TypeName:
			return MaxAggregator[int]{columnName: aggregationColumn, columnType: TypeInt}, nil
		case TypeFloat.TypeName:
			return MaxAggregator[float64]{columnName: aggregationColumn, columnType: TypeFloat}, nil
		default:
			return nil, fmt.Errorf("cannot aggregate type %s", column.ColumnType.Name())
		}
	case AggMin:
		switch column.ColumnType.Name() {
		case TypeInt.TypeName:
			return MinAggregator[int]{columnName: aggregationColumn, columnType: TypeInt}, nil
		case TypeFloat.TypeName:
			return MinAggregator[float64]{columnName: aggregationColumn, columnType: TypeFloat}, nil
		default:
			return nil, fmt.Errorf("cannot aggregate type %s", column.ColumnType.Name())
		}
	}
	return nil, fmt.Errorf("unknown aggregation type %s", aggregationType)
}

func ParseGroup(columnName string, scheme Scheme) (string, error) {
	if _, ok := scheme.Columns[columnName]; !ok {
		return "", fmt.Errorf("column does not exists")
	}
	return columnName, nil
}
