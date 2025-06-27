package csv

import (
	"fmt"
	"strconv"
)

// Limitation on the types used
type Ordered interface {
	~int | ~float64 | ~string
}

// Column type interface for storage versatility
type ColumnTypeInterface interface {
	Name() string
	Parse(string) (any, error)
	Compare(aRaw, bRaw string, cmp comparisonType) (bool, error)
}

type ColumnType[T Ordered] struct {
	TypeName string
	ParseFn  func(string) (T, error)
	CmpFns   map[comparisonType]func(a, b T) bool
}

func (ct ColumnType[T]) Name() string {
	return ct.TypeName
}

func (ct ColumnType[T]) Parse(s string) (any, error) {
	return ct.ParseFn(s)
}

func (ct ColumnType[T]) ParseTyped(s string) (T, error) {
	return ct.ParseFn(s)
}

func (ct ColumnType[T]) Compare(aRaw, bRaw string, cmp comparisonType) (bool, error) {
	a, err := ct.ParseFn(aRaw)
	if err != nil {
		return false, err
	}
	b, err := ct.ParseFn(bRaw)
	if err != nil {
		return false, err
	}
	cmpFunc, ok := ct.CmpFns[cmp]
	if !ok {
		return false, fmt.Errorf("unknown comparison type")
	}
	return cmpFunc(a, b), nil
}

// CSV file schema
type Scheme struct {
	Headers []string              // For the order of columns
	Columns map[string]ColumnInfo // For storing index and column type
}

type ColumnInfo struct {
	Index      int
	ColumnType ColumnTypeInterface
}

type comparisonType int

// ComparisonType defines the type of comparison for filtering CSV data
const (
	Equal comparisonType = iota
	NonEqual
	GreaterThan
	GreaterOrEqual
	LessThan
	LessOrEqual
)

var (
	TypeInt = &ColumnType[int]{
		TypeName: "int",
		ParseFn: func(s string) (int, error) {
			i, err := strconv.Atoi(s)
			return i, err
		},
		CmpFns: defaultCompareFuncs[int](),
	}
	TypeFloat = &ColumnType[float64]{
		TypeName: "float",
		ParseFn: func(s string) (float64, error) {
			f, err := strconv.ParseFloat(s, 64)
			return f, err
		},
		CmpFns: defaultCompareFuncs[float64](),
	}
	TypeString = &ColumnType[string]{
		TypeName: "string",
		ParseFn: func(s string) (string, error) {
			return s, nil
		},
		CmpFns: defaultCompareFuncs[string](),
	}
)

func defaultCompareFuncs[T Ordered]() map[comparisonType]func(a, b T) bool {
	return map[comparisonType]func(a T, b T) bool{
		Equal:          func(a, b T) bool { return a == b },
		NonEqual:       func(a, b T) bool { return a != b },
		GreaterThan:    func(a, b T) bool { return a > b },
		GreaterOrEqual: func(a, b T) bool { return a >= b },
		LessThan:       func(a, b T) bool { return a < b },
		LessOrEqual:    func(a, b T) bool { return a <= b },
	}
}

type Filter struct {
	column          string         // Column of CSV data
	comparisonType  comparisonType // Type of comparison between the value in the column and the control value
	comparisonValue string         // Control value for comparison
}
