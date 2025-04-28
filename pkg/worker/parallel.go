package worker

import (
	"hodctl/pkg/io"
	"log"
	"sync"
)

const (
	ChannelBufferSize = 100 // Size of the Bounded channel
)

// AggResult is the result of processing a batch of transactions by a worker send to the output channel.
type AggResult struct {
	Agg []Agg
	Err error
}

// Do is the function signature for any worker function.
// For now we only have a Agg worker function
type Do = func(batch io.MicroBatch, chOutlier chan Outlier, currencyValues *io.Currency2Values) ([]Agg, error)

// ParallelProcessing distributes the load to NumWorkers workers,
// ensuring only one worker processes each transaction at a time.
func ParallelProcessing(chInput <-chan io.MicroBatch, chOutlier chan Outlier, worker Do, currencyValues *io.Currency2Values, parallelism int) <-chan AggResult {
	aggChan := make(chan AggResult, ChannelBufferSize)

	// WaitGroup for workers, to ensure all workers are done before closing the aggChan
	var wg sync.WaitGroup

	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go Worker(chInput, aggChan, chOutlier, &wg, currencyValues, worker)
	}

	// Close the aggChan when all workers are done
	go func() {
		wg.Wait()
		close(aggChan)
		log.Printf("All workers done, closing agg chan\n")
	}()

	log.Printf("Launched %d workers, returning agg chan\n", parallelism)
	return aggChan
}

// Worker processes each micro-batch and sends the result to the output channel or the outlier channel.
func Worker(in <-chan io.MicroBatch, out chan AggResult, chOutlier chan Outlier, wg *sync.WaitGroup, currencyValues *io.Currency2Values, worker Do) {
	defer wg.Done()

	for batch := range in {
		// Process each batch and generate Agg
		agg, err := worker(batch, chOutlier, currencyValues)
		if err != nil {
			out <- AggResult{Err: err}
			continue
		}
		out <- AggResult{Agg: agg}
	}
}
