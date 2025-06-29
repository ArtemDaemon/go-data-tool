package csv

import (
	"fmt"
	"reflect"
)

type Aggregator interface {
	Name() string                     // {aggregationType_column} like count_age
	Column() string                   // column name
	AggregationType() AggregationType // sum, avg, mix, max, count, countd (count distinct)
	Aggregate([]string) (any, error)
}

type AggregationType string

const (
	AggSum           AggregationType = "sum"
	AggAvg           AggregationType = "avg"
	AggCount         AggregationType = "count"
	AggCountDistinct AggregationType = "countd"
	AggMin           AggregationType = "min"
	AggMax           AggregationType = "max"
)

type SumAggregator[T Numeric] struct {
	columnName string
	columnType *ColumnType[T]
}

func (a SumAggregator[T]) Name() string {
	return fmt.Sprintf("%s_%s", a.columnName, AggSum)
}

func (a SumAggregator[T]) Column() string {
	return a.columnName
}

func (a SumAggregator[T]) AggregationType() AggregationType {
	return AggSum
}

func (a SumAggregator[T]) Aggregate(values []string) (any, error) {
	var sum T
	for _, s := range values {
		v, err := a.columnType.ParseTyped(s)
		if err != nil {
			return nil, err
		}
		sum += v
	}
	return sum, nil
}

type AvgAggregator[T Numeric] struct {
	columnName string
	columnType *ColumnType[T]
}

func (a AvgAggregator[T]) Name() string {
	return fmt.Sprintf("%s_%s", a.columnName, AggAvg)
}

func (a AvgAggregator[T]) Column() string {
	return a.columnName
}

func (a AvgAggregator[T]) AggregationType() AggregationType {
	return AggAvg
}

func (a AvgAggregator[T]) Aggregate(values []string) (any, error) {
	var sum T
	for _, s := range values {
		v, err := a.columnType.ParseTyped(s)
		if err != nil {
			return nil, err
		}
		sum += v
	}
	return float64(sum) / float64(len(values)), nil
}

type MaxAggregator[T Ordered] struct {
	columnName string
	columnType *ColumnType[T]
}

func (a MaxAggregator[T]) Name() string {
	return fmt.Sprintf("%s_%s", a.columnName, AggMax)
}

func (a MaxAggregator[T]) Column() string {
	return a.columnName
}

func (a MaxAggregator[T]) AggregationType() AggregationType {
	return AggMax
}

func (a MaxAggregator[T]) Aggregate(values []string) (any, error) {
	var max T
	for _, s := range values {
		v, err := a.columnType.ParseTyped(s)
		if err != nil {
			return nil, err
		}
		if isEmpty(max) {
			max = v
		} else if v > max {
			max = v
		}
	}
	return max, nil
}

type MinAggregator[T Ordered] struct {
	columnName string
	columnType *ColumnType[T]
}

func (a MinAggregator[T]) Name() string {
	return fmt.Sprintf("%s_%s", a.columnName, AggMin)
}

func (a MinAggregator[T]) Column() string {
	return a.columnName
}

func (a MinAggregator[T]) AggregationType() AggregationType {
	return AggMin
}

func (a MinAggregator[T]) Aggregate(values []string) (any, error) {
	var min T
	for _, s := range values {
		v, err := a.columnType.ParseTyped(s)
		if err != nil {
			return nil, err
		}
		if isEmpty(min) {
			min = v
		} else if v < min {
			min = v
		}
	}
	return min, nil
}

type CountAggregator[T Ordered] struct {
	columnName string
}

func (a CountAggregator[T]) Name() string {
	return fmt.Sprintf("%s_%s", a.columnName, AggCount)
}

func (a CountAggregator[T]) Column() string {
	return a.columnName
}

func (a CountAggregator[T]) AggregationType() AggregationType {
	return AggCount
}

func (a CountAggregator[T]) Aggregate(values []string) (any, error) {
	return len(values), nil
}

type CountDistinctAggregator[T Ordered] struct {
	columnName string
}

func (a CountDistinctAggregator[T]) Name() string {
	return fmt.Sprintf("%s_%s", a.columnName, AggCountDistinct)
}

func (a CountDistinctAggregator[T]) Column() string {
	return a.columnName
}

func (a CountDistinctAggregator[T]) AggregationType() AggregationType {
	return AggCountDistinct
}

func (a CountDistinctAggregator[T]) Aggregate(values []string) (any, error) {
	valuesMap := make(map[string]bool)
	for _, v := range values {
		valuesMap[v] = true
	}
	return len(valuesMap), nil
}

func isEmpty[T any](v T) bool {
	return reflect.ValueOf(v).IsZero()
}
