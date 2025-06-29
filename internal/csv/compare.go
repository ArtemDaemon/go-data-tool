package csv

import "strconv"

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
