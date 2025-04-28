package cmd

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func benchmarkAgg(b *testing.B, parallelism int, microBatchSize int) {
	b.ResetTimer()

	inputCurrencyValue := "../testdata/currencies_usd.csv"
	inputTransactions := "../testdata/big_sample_data.csv"
	aggOutput := "../testdata/output.csv"
	errOutput := "../testdata/errors.csv"

	for i := 0; i < b.N; i++ {
		err := aggregateTransactions(inputCurrencyValue, inputTransactions, aggOutput, errOutput, parallelism, microBatchSize)
		if err != nil {
			b.Fatalf("Error during benchmark: %v", err)
		}
	}
	fmt.Printf(" * Benchmark result for parallelism: %d, microBatchSize: %d, %f seconds/op\n", parallelism, microBatchSize, (b.Elapsed() / time.Duration(b.N)).Seconds())
}

func Benchmark_AggregateTransactions(b *testing.B) {
	// Benchmark for different parallelism and microbatch sizes
	parallelismOptions := []int{1, runtime.NumCPU(), 100}
	microBatchSizeOptions := []int{1, 100, 1000, 10000, 100000}

	for _, p := range parallelismOptions {
		for _, m := range microBatchSizeOptions {
			b.Run(fmt.Sprintf("Parallelism=%d/MicroBatchSize=%d", p, m), func(b *testing.B) {
				benchmarkAgg(b, p, m)
			})
		}
	}
}
