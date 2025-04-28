package pipeline

import (
	"fmt"
	"hodctl/pkg/io"
	"hodctl/pkg/sink"
	"hodctl/pkg/worker"
	stdio "io"
	"log"
)

type Do = func(batch io.MicroBatch, currencyValues *io.Currency2Values) ([]worker.Agg, <-chan worker.Outlier, error)

const ChannelBufferSize = 100

func DoAgg(currencyReader stdio.Reader, transactionsReader stdio.Reader, aggSink sink.AggSink, errSink sink.ErrSink, parallelism int, microBatchSize int) error {
	currencyValues, err := io.ReadCurrencyValues(currencyReader)
	if err != nil {
		return fmt.Errorf("failed to read currency values: %v", err)
	}
	log.Printf("Read %d currency values\n", len(currencyValues))

	sourceTransactionCh, err := io.ReadCSV(transactionsReader, microBatchSize)
	if err != nil {
		return fmt.Errorf("failed to read transactions: %v", err)
	}

	// Create the outlier channel
	outlierCh := make(chan worker.Outlier, ChannelBufferSize)
	go errSink.WriteError(outlierCh)

	partialAgg := worker.ParallelProcessing(sourceTransactionCh, outlierCh, DoAggBatch, &currencyValues, parallelism)

	// reduce
	agg, err := worker.DoAggReducer(partialAgg)
	if err != nil {
		return fmt.Errorf("failed to reduce aggregated transactions: %v", err)
	}

	log.Printf("Generated %d aggregated transactions\n", len(agg))
	size, err := aggSink.WriteAgg(agg)
	if err != nil {
		return fmt.Errorf("failed to write aggregated transactions: %v", err)
	}
	log.Printf("Wrote %d Agg to sink\n", size)
	close(outlierCh)

	return nil
}

// DoAggBatch processes a batch of transactions and returns the aggregated results.
func DoAggBatch(batch io.MicroBatch, outlierChan chan worker.Outlier, currencyValues *io.Currency2Values) ([]worker.Agg, error) {

	// Clean up the batch
	cleanedBatch, err := worker.DoCleanup(batch, outlierChan)
	if err != nil {
		return nil, fmt.Errorf("failed to clean up batch: %v", err)
	}

	// Do Aggregation
	agg, err := worker.DoAgg(cleanedBatch, currencyValues)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate batch: %v", err)
	}

	return agg, nil
}
