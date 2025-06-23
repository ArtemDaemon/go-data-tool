package cmd

import (
	"errors"
	"go-data-tool/internal/csv"
	"log"
	"os"

	"github.com/spf13/cobra"
)

var (
	input   string   // input file
	output  string   // output file
	filters []string // slice of installed filters
)

var parseCmd = &cobra.Command{
	Use:   "parse",
	Short: "Processing CSV data",
	Long:  `The parse command allows you to read CSV file data, perform filtering, column selection, grouping, and aggregation.`,
	Run: func(cmd *cobra.Command, args []string) {
		var records [][]string
		var parsedFilters []csv.Filter

		// Process filters
		for _, filter := range filters {
			parsedFilter, err := csv.ParseFilter(filter)
			if err != nil {
				log.Fatalf("Filter '%s' parsing error: %s", filter, err)
			}
			parsedFilters = append(parsedFilters, *parsedFilter)
		}

		// Check if input flag is not empty and check existance of file
		if input != "" {
			var err error
			if _, err = os.Stat(input); err != nil && errors.Is(err, os.ErrNotExist) {
				log.Fatal("Input file not found")
			}
			records, err = csv.ParseCSV(input, parsedFilters)
			if err != nil {
				log.Fatal("Error parsing csv file: ", err)
			}
		}
		// TODO: Add the ability to parse data passed through the pipeline

		if output != "" {
			err := csv.SaveCSV(records, output)
			if err != nil {
				log.Fatal("Error saving csv file", err)
			}
		}
		// TODO: Add the ability to pass data through the pipeline

		log.Println("CSV data was processed correctly")
	},
}

func init() {
	rootCmd.AddCommand(parseCmd)
	parseCmd.Flags().StringVarP(&input, "input", "i", "", "file address for processing")
	parseCmd.Flags().StringVarP(&output, "output", "o", "", "output file address")
	parseCmd.Flags().StringSliceVarP(&filters, "filter", "f", []string{}, `set of filters in the format "column operation value"
can be passed in by separating them with commas or by reusing the flag
values for comparison by greater than and less than operations must be numeric
possible operations: =, !=, >, >=, <, <=`)
}
