package cmd

import (
	"github.com/spf13/cobra"
)

var (
	input  string // input file
	output string //output file
)

var parseCmd = &cobra.Command{
	Use:   "parse",
	Short: "Processing CSV data",
	Long:  `The parse command allows you to read CSV file data, perform filtering, column selection, grouping, and aggregation.`,
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	rootCmd.AddCommand(parseCmd)
	parseCmd.Flags().StringVarP(&input, "input", "i", "", "file address for processing")
	parseCmd.Flags().StringVarP(&output, "output", "o", "output.csv", "output file address")
}
