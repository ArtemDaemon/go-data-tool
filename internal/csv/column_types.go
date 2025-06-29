package csv

import "fmt"

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
