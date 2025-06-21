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

		// Check if input flag is not empty and check existance of file
		if input != "" {
			if _, err := os.Stat(input); err != nil && errors.Is(err, os.ErrNotExist) {
				log.Fatal("Input file not found")
			}
			records = csv.ParseCSV(input)
		}
		// TODO: Add the ability to parse data passed through the pipeline

		if output != "" {
			csv.SaveCSV(records, output)
		}
		// TODO: Add the ability to pass data through the pipeline

		log.Println("CSV data was processed correctly")
	},
}

func init() {
	rootCmd.AddCommand(parseCmd)
	parseCmd.Flags().StringVarP(&input, "input", "i", "", "file address for processing")
	parseCmd.Flags().StringVarP(&output, "output", "o", "", "output file address")
	parseCmd.Flags().StringSliceVarP(&filters, "filter", "f", []string{}, "set of filters in the format key=value; can be passed in by separating them with commas or by reusing the flag")
}
