package io

import (
	"fmt"
	"io"
	"log"

	"github.com/gocarina/gocsv"
)

const (
	DefaultChannelBufferSize = 100 // Default buffer size for the channel
)

type MicroBatch struct {
	Data []RawTransaction
	Err  error
}

type RawTransaction struct {
	Timestamp string `csv:"ts"`
	ProjectID string `csv:"project_id"`
	Event     string `csv:"event"`
	Props     string `csv:"props"`
	Nums      string `csv:"nums"`
}

// ReadCSV reads the CSV data in batches and returns a channel that emits MicroBatch structs.
// The function also returns an error if any occurs during reading.
func ReadCSV(reader io.Reader, microBatchSize int) (<-chan MicroBatch, error) {
	dataCh := make(chan MicroBatch, DefaultChannelBufferSize)

	go func() {
		defer close(dataCh)

		var batch []RawTransaction
		numTransactions := 0

		err := gocsv.UnmarshalToCallbackWithError(reader, func(rt RawTransaction) error {
			numTransactions++
			batch = append(batch, rt)

			// If the batch size is reached, send the batch and reset
			if len(batch) >= microBatchSize {
				dataCh <- MicroBatch{Data: batch}
				batch = make([]RawTransaction, 0, microBatchSize) // Reset the batch with capacity
			}
			return nil
		})

		if err != nil {
			dataCh <- MicroBatch{
				Err: fmt.Errorf("an error occured while reading the CSV, error: %v", err),
			}
			fmt.Printf("CSV Parse Error in line %d : %v\n", numTransactions, err)
		}
		if len(batch) > 0 {
			dataCh <- MicroBatch{Data: batch}
		}

		log.Printf("CSV read done, source channel closed after reading %d transactions\n", numTransactions)
	}()

	return dataCh, nil
}
