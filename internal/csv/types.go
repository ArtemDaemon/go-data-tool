package csv

// Limitation on the types used
type Ordered interface {
	~int | ~float64 | ~string
}
type Numeric interface {
	~int | ~float64
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

type Filter struct {
	column          string         // Column of CSV data
	comparisonType  comparisonType // Type of comparison between the value in the column and the control value
	comparisonValue string         // Control value for comparison
}
