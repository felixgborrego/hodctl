package worker

import (
	"fmt"
	"hodctl/pkg/io"
)

type Agg struct {
	Date                 string
	ProjectId            string
	NumberOfTransactions int
	TotalVolumeUsd       float64
}

// ProcessRawTransaction processes a batch of transactions, groups by date and projectId, and aggregates the data
func DoAgg(batch MicroBatch, currencyValues *io.Currency2Values) ([]Agg, error) {
	// Map to aggregate results
	aggMap := make(map[string]Agg)

	for _, transaction := range batch.Data {

		// convert to usd
		toUsd, exists := (*currencyValues)[transaction.CurrencySymbol]
		if !exists {
			return nil, fmt.Errorf("currency symbol not supported: %s", transaction.CurrencySymbol)
		}

		volume := toUsd * transaction.Volume

		date := transaction.Timestamp.Format("2006-01-02")

		// Create a unique key for grouping by date and projectId
		key := date + "_" + transaction.ProjectID

		// Update the aggregate map for the given key (date + projectId)
		agg, exists := aggMap[key]
		if !exists {
			agg = Agg{
				Date:      date,
				ProjectId: transaction.ProjectID,
			}
		}

		// Increment the number of transactions and sum the total volume
		agg.NumberOfTransactions++
		agg.TotalVolumeUsd += volume

		// Store the updated aggregate back in the map
		aggMap[key] = agg
	}

	// Convert the map to a slice of Agg
	var aggs []Agg
	for _, agg := range aggMap {
		aggs = append(aggs, agg)
	}

	return aggs, nil
}

// DoAggReducer collects all Agg from workers and combines them (reduce step)
func DoAggReducer(in <-chan AggResult) ([]Agg, error) {
	aggMap := make(map[string]Agg)

	// Combine all incoming Agg results
	for result := range in {
		if result.Err != nil {
			return nil, fmt.Errorf("unable to aggregate transactions: %v", result.Err)
		}

		for _, agg := range result.Agg {

			key := agg.Date + "_" + agg.ProjectId

			existingAgg, exists := aggMap[key]
			if !exists {
				aggMap[key] = agg
			} else {
				// Accumulate the result
				existingAgg.NumberOfTransactions += agg.NumberOfTransactions
				existingAgg.TotalVolumeUsd += agg.TotalVolumeUsd
				aggMap[key] = existingAgg
			}
		}
	}

	// Convert map to slice
	var result []Agg
	for _, agg := range aggMap {
		result = append(result, agg)
	}

	return result, nil
}
