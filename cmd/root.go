package cmd

import (
	"fmt"
	"os"

	"github.com/gookit/color"
	"github.com/spf13/cobra"
)

const version = "v0.1.0"

var rootCmd = &cobra.Command{
	Use:   "hodctl",
	Short: "Experimental Data Tooling CLI",
	Long:  "Tool for data processing for the Experimental Data platform",
}

func Execute() {
	displayBanner()
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func displayBanner() {
	reflekColor := color.HEX("#FF2E55")
	fmt.Printf("Welcome to ")
	reflekColor.Printf("Experimental Data Tooling")
	fmt.Printf(" CLI %s!\n", version)
}
