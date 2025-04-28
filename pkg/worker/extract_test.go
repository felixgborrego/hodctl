package worker

import (
	"strings"
	"testing"
	"time"

	"hodctl/pkg/io"
)

var rawTransactions = []io.RawTransaction{
	{
		Timestamp: "2023-10-01 12:00:00",
		ProjectID: "ProjectA",
		Nums:      `{"currencyValueDecimal": "100.0"}`,
		Props:     `{"currencySymbol": "USD"}`,
	},
	{
		Timestamp: "2023-10-01 13:00:00",
		ProjectID: "ProjectB",
		Nums:      `{"currencyValueDecimal": "50.0"}`,
		Props:     `{"currencySymbol": "EUR"}`,
	},
	{
		Timestamp: "invalid-timestamp",
		ProjectID: "ProjectC",
		Nums:      `{"currencyValueDecimal": "150.0"}`,
		Props:     `{"currencySymbol": "GBP"}`,
	},
	{
		Timestamp: "2023-10-01 14:00:00",
		ProjectID: "ProjectD",
		Nums:      `invalid-json`,
		Props:     `{"currencySymbol": "JPY"}`,
	},
	{
		Timestamp: "2023-10-01 15:00:00",
		ProjectID: "ProjectE",
		Nums:      `{"currencyValueDecimal": "-100.0"}`,
		Props:     `{"currencySymbol": "CAD"}`,
	},
	{
		Timestamp: "2023-10-01 16:00:00",
		ProjectID: "ProjectF",
		Nums:      `{"currencyValueDecimal": "1e20"}`, // Exceeds MaxVolumeThreshold
		Props:     `{"currencySymbol": "AUD"}`,
	},
	{
		Timestamp: "2023-10-01 17:00:00",
		ProjectID: "ProjectG",
		Nums:      `{"currencyValueDecimal": "200.0"}`,
		Props:     `{"currencySymbol": "CHF"}`,
	},
}

// Define expected cleaned transactions
var expectedTransactions = []Transaction{
	{
		Timestamp:      time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
		ProjectID:      "ProjectA",
		CurrencySymbol: "USD",
		Volume:         100.0,
	},
	{
		Timestamp:      time.Date(2023, 10, 1, 13, 0, 0, 0, time.UTC),
		ProjectID:      "ProjectB",
		CurrencySymbol: "EUR",
		Volume:         50.0,
	},
	{
		Timestamp:      time.Date(2023, 10, 1, 17, 0, 0, 0, time.UTC),
		ProjectID:      "ProjectG",
		CurrencySymbol: "CHF",
		Volume:         200.0,
	},
}

func TestDoCleanup(t *testing.T) {
	// Prepare test data with a mix of valid and invalid transactions

	batch := io.MicroBatch{
		Data: rawTransactions,
		Err:  nil,
	}

	outlierChan := make(chan Outlier, len(rawTransactions))

	// TEST Call DoCleanup
	cleanedBatch, err := DoCleanup(batch, outlierChan)
	if err != nil {
		t.Fatalf("DoCleanup returned error: %v", err)
	}

	close(outlierChan)

	// Collect outliers
	var outliers []Outlier
	for outlier := range outlierChan {
		outliers = append(outliers, outlier)
	}

	// Verify that the cleaned transactions match the expected transactions
	if len(cleanedBatch.Data) != len(expectedTransactions) {
		t.Errorf("Expected %d cleaned transactions, got %d", len(expectedTransactions), len(cleanedBatch.Data))
	}

	for i, expected := range expectedTransactions {
		actual := cleanedBatch.Data[i]
		if !actual.Timestamp.Equal(expected.Timestamp) {
			t.Errorf("Transaction %d: expected Timestamp %v, got %v", i, expected.Timestamp, actual.Timestamp)
		}
		if actual.ProjectID != expected.ProjectID {
			t.Errorf("Transaction %d: expected ProjectID %s, got %s", i, expected.ProjectID, actual.ProjectID)
		}
		if actual.CurrencySymbol != expected.CurrencySymbol {
			t.Errorf("Transaction %d: expected CurrencySymbol %s, got %s", i, expected.CurrencySymbol, actual.CurrencySymbol)
		}
		if actual.Volume != expected.Volume {
			t.Errorf("Transaction %d: expected Volume %f, got %f", i, expected.Volume, actual.Volume)
		}
	}

	// Define expected outlier reasons mapped by ProjectID
	expectedOutlierReasons := map[string]string{
		"ProjectC": "invalid timestamp format",
		"ProjectD": "invalid JSON format in Nums field",
		"ProjectE": "volume over threshold",
		"ProjectF": "volume over threshold",
	}

	// Verify that the outliers match the expected ones
	if len(outliers) != len(expectedOutlierReasons) {
		t.Errorf("Expected %d outliers, got %d", len(expectedOutlierReasons), len(outliers))
	}

	for _, outlier := range outliers {
		expectedReason, exists := expectedOutlierReasons[outlier.ProjectID]
		if !exists {
			t.Errorf("Unexpected outlier for ProjectID %s", outlier.ProjectID)
			continue
		}
		if !strings.Contains(outlier.Reason, expectedReason) {
			t.Errorf("Outlier for ProjectID %s: expected reason to contain '%s', got '%s'", outlier.ProjectID, expectedReason, outlier.Reason)
		}
	}
}
