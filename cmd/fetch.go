package cmd

import (
	"fmt"
	"hodctl/pkg/io"
	"log"
	"os"

	"github.com/spf13/cobra"
)

// FetchArgs ars for the 'fetch' command
type FetchArgs struct {
	apikey string // API key for CoinGecko
	output string // Path to the output CSV file
}

var fetchArgs FetchArgs

var fetchCmd = &cobra.Command{
	Use:   "fetch",
	Short: "Preload all the currency conversion rates in USD and save as CSV",
	Long:  "Fetch the value of all currencies from the CoinGecko API and save it as a CSV file",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Fetching currency conversion rates with API key: %s\n", fetchArgs.apikey[:5]+"****")
		fmt.Printf("Saving results to: %s\n", fetchArgs.output)
		if err := fetchAndSaveConversionRates(fetchArgs.apikey, fetchArgs.output); err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching and saving conversion rates: %v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	// Define the flags for the 'fetch' command
	fetchCmd.Flags().StringVarP(&fetchArgs.apikey, "apikey", "a", "", "API key for CoinGecko (required)")
	fetchCmd.Flags().StringVarP(&fetchArgs.output, "output", "o", "", "Path to save the CSV file (required)")

	// Mark the flags as required
	fetchCmd.MarkFlagRequired("apikey")
	fetchCmd.MarkFlagRequired("output")

	rootCmd.AddCommand(fetchCmd)
}

func fetchAndSaveConversionRates(apikey string, output string) error {

	client, err := io.NewCoinGeckoClient(apikey)
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}
	log.Printf("Connected to CoinGecko API\n")

	writer, err := io.Open(output)
	if err != nil {
		return err
	}

	err = client.Save(writer)
	if err != nil {
		return err
	}
	return writer.Close()
}
