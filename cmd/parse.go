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
	sum     []string // slice of columns for sum aggregation
)

var parseCmd = &cobra.Command{
	Use:   "parse",
	Short: "Processing CSV data",
	Long:  `The parse command allows you to read CSV file data, perform filtering, column selection, grouping, and aggregation.`,
	Run: func(cmd *cobra.Command, args []string) {
		var parsedFilters []csv.Filter
		var parsedAggregations []csv.Aggregation

		// Check if input flag is not empty and check existance of file
		if input != "" {
			if _, err := os.Stat(input); err != nil && errors.Is(err, os.ErrNotExist) {
				log.Fatal("Input file not found")
			}
		}
		// TODO: Add the ability to parse data passed through the pipeline

		// Reading the CSV file structure
		log.Println("Parsing file structure...")
		scheme, err := csv.ParseCSVStructure(input)
		if err != nil {
			log.Fatal("Error parsing csv structure: ", err)
		}

		// Process filters
		if len(filters) != 0 {
			log.Println("Parsing filters...")

			for _, filter := range filters {
				parsedFilter, err := csv.ParseFilter(filter, scheme)
				if err != nil {
					log.Fatalf("Filter '%s' parsing error: %s", filter, err)
				}
				parsedFilters = append(parsedFilters, parsedFilter)
			}
		}

		// Process aggregations
		if len(sum) != 0 {
			log.Println("Pasing aggregations...")
			for _, column := range sum {
				parsedAggregation, err := csv.ParseAggregation(column, csv.Sum, scheme)
				if err != nil {
					log.Fatalf("Aggregation sum('%s') parsing error: %s", column, err)
				}
				parsedAggregations = append(parsedAggregations, parsedAggregation)
			}
		}

		log.Println("Parsing file...")
		records, err := csv.ParseCSV(input, scheme, parsedFilters, parsedAggregations)
		if err != nil {
			log.Fatal("Error parsing csv file: ", err)
		}
		// TODO: Add the ability to parse data passed through the pipeline

		log.Println("Saving proccessed data...")
		err = csv.SaveCSV(records, output)
		if err != nil {
			log.Fatal("Error saving csv file", err)
		}
		// TODO: Add the ability to pass data through the pipeline

		log.Println("CSV data was processed correctly")
	},
}

func init() {
	rootCmd.AddCommand(parseCmd)
	parseCmd.Flags().StringVarP(&input, "input", "i", "", "file address for processing (required)")
	parseCmd.MarkFlagRequired("input")

	parseCmd.Flags().StringVarP(&output, "output", "o", "", "output file address (required)")
	parseCmd.MarkFlagRequired("output")

	parseCmd.Flags().StringSliceVarP(&filters, "filter", "f", []string{}, `set of filters in the format "column operation value"
can be passed in by separating them with commas or by reusing the flag
possible operations: =, !=, >, >=, <, <=`)

	parseCmd.Flags().StringSliceVarP(&sum, "sum", "s", []string{}, "set of columns for sum aggregation")
}
