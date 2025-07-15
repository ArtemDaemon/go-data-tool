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
	avg     []string // slice of columns for avg aggregation
	max     []string // slice of columns for max aggregation
	min     []string // slice of columns for min aggregation
	count   []string // slice of columns for count aggregation
	countd  []string // slice of columns for cound distinct aggregation
)

var parseCmd = &cobra.Command{
	Use:   "parse",
	Short: "Processing CSV data",
	Long:  `The parse command allows you to read CSV file data, perform filtering, column selection, grouping, and aggregation.`,
	Run: func(cmd *cobra.Command, args []string) {
		var parsedFilters []csv.Filter
		var parsedAggregations []csv.Aggregator

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
		if len(sum) != 0 || len(avg) != 0 || len(max) != 0 || len(min) != 0 || len(count) != 0 || len(countd) != 0 {
			log.Println("Pasing aggregations...")
			for _, column := range sum {
				parsedAggregation, err := csv.ParseAggregation(column, csv.AggSum, scheme)
				if err != nil {
					log.Fatalf("Aggregation sum('%s') parsing error: %s", column, err)
				}
				parsedAggregations = append(parsedAggregations, parsedAggregation)
			}
			for _, column := range avg {
				parsedAggregation, err := csv.ParseAggregation(column, csv.AggAvg, scheme)
				if err != nil {
					log.Fatalf("Aggregation avg('%s') parsing error: %s", column, err)
				}
				parsedAggregations = append(parsedAggregations, parsedAggregation)
			}
			for _, column := range max {
				parsedAggregation, err := csv.ParseAggregation(column, csv.AggMax, scheme)
				if err != nil {
					log.Fatalf("Aggregation max('%s') parsing error: %s", column, err)
				}
				parsedAggregations = append(parsedAggregations, parsedAggregation)
			}
			for _, column := range min {
				parsedAggregation, err := csv.ParseAggregation(column, csv.AggMin, scheme)
				if err != nil {
					log.Fatalf("Aggregation min('%s') parsing error: %s", column, err)
				}
				parsedAggregations = append(parsedAggregations, parsedAggregation)
			}
			for _, column := range count {
				parsedAggregation, err := csv.ParseAggregation(column, csv.AggCount, scheme)
				if err != nil {
					log.Fatalf("Aggregation count('%s') parsing error: %s", column, err)
				}
				parsedAggregations = append(parsedAggregations, parsedAggregation)
			}
			for _, column := range countd {
				parsedAggregation, err := csv.ParseAggregation(column, csv.AggCountDistinct, scheme)
				if err != nil {
					log.Fatalf("Aggregation countd('%s') parsing error: %s", column, err)
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

	parseCmd.Flags().StringSliceVarP(&sum, "sum", "s", []string{}, "set of columns for 'sum' aggregation")
	parseCmd.Flags().StringSliceVarP(&avg, "avg", "a", []string{}, "set of columns for 'avg' aggregation")
	parseCmd.Flags().StringSliceVarP(&max, "max", "M", []string{}, "set of columns for 'max' aggregation")
	parseCmd.Flags().StringSliceVarP(&min, "min", "m", []string{}, "set of columns for 'min' aggregation")
	parseCmd.Flags().StringSliceVarP(&count, "count", "c", []string{}, "set of columns for 'count' aggregation")
	parseCmd.Flags().StringSliceVarP(&countd, "countd", "C", []string{}, "set of columns for 'count distinct' aggregation")
}
