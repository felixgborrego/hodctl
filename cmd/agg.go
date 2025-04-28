package cmd

import (
	"context"
	"fmt"
	"hodctl/pkg/io"
	"hodctl/pkg/pipeline"
	"hodctl/pkg/sink"
	stdio "io"
	"log"
	"runtime"
	"time"

	"github.com/spf13/cobra"
)

type AggArgs struct {
	InputCurrencyValue string // Path to the currency value CSV file
	InputTransactions  string // Path to the transactions CSV file
	Output             string // Path to the output
	OutputErr          string // Path to the error output
	Parallelism        int    // Number of goroutines for parallel processing
	MicroBatchSize     int    // Size of each micro-batch for processing
}

var aggArgs AggArgs

const (
	// DefaultMicroBatchSize Default size of each micro-batch for processing
	// a Batch size of 100K records is the Optimal size for processing large CSV files as the Benchmark tests have shown but has high memory usage (~300Mb)
	// a Batch size of 10K records is the Optimal size for processing small CSV files as the Benchmark tests have shown
	DefaultMicroBatchSize = 10_000 // (10K records) offers a good balance between memory usage (~40Mb) and processing speed

	// Hard limit for processing time to avoid hanging processes burning unecesary resources in case something goes terribly wrong.
	// TODO adjust based on actual load (or parametrize) to a maximum reasonable time we never expect to exceed.
	MaxProcessTime = 30 * time.Minute
)

var DefaultParallelism = runtime.NumCPU()

var aggCmd = &cobra.Command{
	Use:   "agg",
	Short: "Aggregate transactions",
	Long:  `Aggregate the transaction data with currency conversion rates, and save the results as output`,
	Run:   runAggregationCmd,
}

func init() {
	aggCmd.Flags().StringVarP(&aggArgs.InputCurrencyValue, "input-currencies", "c", "", "Path to the currency value CSV file (gs, s3, local file system) (required)")
	aggCmd.Flags().StringVarP(&aggArgs.InputTransactions, "input-transactions", "t", "", "Path to the transactions CSV file (gs, s3, local file system) (required)")
	aggCmd.Flags().StringVarP(&aggArgs.Output, "output", "o", "", "Path to save the aggregated result (BigQuery, gs, s3, local file system) (required)")
	aggCmd.Flags().StringVarP(&aggArgs.OutputErr, "output-error", "e", "", "Path to save the error output CSV file (gs, s3, local file system) (required)")
	aggCmd.Flags().IntVarP(&aggArgs.Parallelism, "parallelism", "p", DefaultParallelism, "Number of goroutines for parallel processing")
	aggCmd.Flags().IntVarP(&aggArgs.MicroBatchSize, "micro-batch-size", "b", DefaultMicroBatchSize, "Size of each microbatch for processing")

	aggCmd.MarkFlagRequired("input-currencies")
	aggCmd.MarkFlagRequired("input-transactions")
	aggCmd.MarkFlagRequired("output")
	aggCmd.MarkFlagRequired("output-error")

	rootCmd.AddCommand(aggCmd)
}

func runAggregationCmd(cmd *cobra.Command, args []string) {
	log.Printf("Starting aggregation with parallelism=%d and microbatch size=%d\n", aggArgs.Parallelism, aggArgs.MicroBatchSize)
	log.Printf("Input currency values: %s", aggArgs.InputCurrencyValue)
	log.Printf("Input transactions: %s", aggArgs.InputTransactions)
	log.Printf("Output results: %s", aggArgs.Output)

	if err := aggregateTransactions(
		aggArgs.InputCurrencyValue,
		aggArgs.InputTransactions,
		aggArgs.Output,
		aggArgs.OutputErr,
		aggArgs.Parallelism,
		aggArgs.MicroBatchSize); err != nil {
		log.Fatalf("Error during aggregation: %v", err)
	}
}

func aggregateTransactions(inputCurrencyValue, inputTransactions, aggOutput, errOutput string, parallelism, microBatchSize int) error {
	ctx, cancel := context.WithTimeout(context.Background(), MaxProcessTime)
	defer cancel()

	// Opening currency value file
	currencyReader, err := io.Open(inputCurrencyValue)
	if err != nil {
		return fmt.Errorf("failed to open currency value file: %w", err)
	}
	defer safeClose(currencyReader, "currencyReader")

	// Opening transactions file
	transactionReader, err := io.Open(inputTransactions)
	if err != nil {
		return fmt.Errorf("failed to open transactions file: %w", err)
	}
	defer safeClose(transactionReader, "transactionReader")

	// Creating sinks
	aggSink, err := sink.NewAggSink(ctx, aggOutput)
	if err != nil {
		return fmt.Errorf("failed to create sink: %w", err)
	}
	defer safeClose(aggSink, "aggSink")

	errSink, err := sink.NewOutlierSink(ctx, errOutput)
	if err != nil {
		return fmt.Errorf("failed to create error sink: %w", err)
	}
	defer safeClose(errSink, "errSink")

	// Start the aggregation process
	return pipeline.DoAgg(currencyReader, transactionReader, aggSink, errSink, parallelism, microBatchSize)
}

func safeClose(closer stdio.Closer, name string) {
	if err := closer.Close(); err != nil {
		log.Printf("Error closing %s: %v", name, err)
	}
}
