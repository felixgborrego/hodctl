package worker

import (
	"testing"
	"time"

	"hodctl/pkg/io"
)

var transactions = []Transaction{
	{
		Timestamp:      time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
		ProjectID:      "ProjectA",
		CurrencySymbol: "USD",
		Volume:         100.0,
	},
	{
		Timestamp:      time.Date(2023, 10, 1, 13, 0, 0, 0, time.UTC),
		ProjectID:      "ProjectA",
		CurrencySymbol: "EUR",
		Volume:         50.0,
	},
	{
		Timestamp:      time.Date(2023, 10, 1, 14, 0, 0, 0, time.UTC),
		ProjectID:      "ProjectB",
		CurrencySymbol: "BTC",
		Volume:         0.001,
	},
	{
		Timestamp:      time.Date(2023, 10, 2, 12, 0, 0, 0, time.UTC),
		ProjectID:      "ProjectA",
		CurrencySymbol: "USD",
		Volume:         200.0,
	},
	{
		Timestamp:      time.Date(2023, 10, 2, 13, 0, 0, 0, time.UTC),
		ProjectID:      "ProjectB",
		CurrencySymbol: "EUR",
		Volume:         80.0,
	},
}

var expectedAggs = []Agg{
	{
		Date:                 "2023-10-01",
		ProjectId:            "ProjectA",
		NumberOfTransactions: 2,
		TotalVolumeUsd:       160.0, // 100*1.0 + 50*1.2
	},
	{
		Date:                 "2023-10-01",
		ProjectId:            "ProjectB",
		NumberOfTransactions: 1,
		TotalVolumeUsd:       60.0, // 0.001*60000
	},
	{
		Date:                 "2023-10-02",
		ProjectId:            "ProjectA",
		NumberOfTransactions: 1,
		TotalVolumeUsd:       200.0, // 200*1.0
	},
	{
		Date:                 "2023-10-02",
		ProjectId:            "ProjectB",
		NumberOfTransactions: 1,
		TotalVolumeUsd:       96.0, // 80*1.2
	},
}

func TestDoAgg(t *testing.T) {
	// Setup test data
	currencyValues := io.Currency2Values{
		"USD": 1.0,
		"EUR": 1.2,
		"BTC": 60000.0,
	}

	// Define sample transactions

	batch := MicroBatch{
		Data: transactions,
	}

	// Call DoAgg
	aggs, err := DoAgg(batch, &currencyValues)
	if err != nil {
		t.Fatalf("DoAgg returned error: %v", err)
	}

	// Convert the Aggs to a map for easy comparisons
	aggsMap := make(map[string]Agg)
	for _, agg := range aggs {
		key := agg.Date + "_" + agg.ProjectId
		aggsMap[key] = agg
	}

	// Compare the results
	for _, expected := range expectedAggs {
		key := expected.Date + "_" + expected.ProjectId
		agg, exists := aggsMap[key]
		if !exists {
			t.Errorf("Expected Agg not found: %+v", expected)
			continue
		}
		if agg.NumberOfTransactions != expected.NumberOfTransactions {
			t.Errorf("Mismatch in NumberOfTransactions for %s: got %d, want %d", key, agg.NumberOfTransactions, expected.NumberOfTransactions)
		}
		if agg.TotalVolumeUsd != expected.TotalVolumeUsd {
			t.Errorf("Mismatch in TotalVolumeUsd for %s: got %f, want %f", key, agg.TotalVolumeUsd, expected.TotalVolumeUsd)
		}
	}

	// Check for unexpected Aggs
	if len(aggs) != len(expectedAggs) {
		t.Errorf("Got %d Aggs, expected %d", len(aggs), len(expectedAggs))
	}
}
