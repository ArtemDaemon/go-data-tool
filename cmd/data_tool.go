/*
Copyright © 2025 ArtemDaemon artem.daemon.official@gmail.com
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "go-data-tool",
	Short: "Universal CLI utility for performing basic data operations",
	Long:  `go-data-tool is a CLI utility for working with CSV files, integrating with external APIs, backup and logging`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
}
