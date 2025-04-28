package worker

import (
	"encoding/json"
	"fmt"
	"hodctl/pkg/io"
	"strconv"
	"time"
)

// Transaction struct to hold a cleaned transaction (after processing raw transactions)
type Transaction struct {
	Timestamp      time.Time
	ProjectID      string
	CurrencySymbol string
	Volume         float64
}

// MicroBatch struct to hold a batch of transactions for processing
type MicroBatch struct {
	Data []Transaction
	Err  error
}

// Outlier struct to hold detected outliers and invalid transactions
type Outlier struct {
	Reason string
	io.RawTransaction
}

type numsJson struct {
	CurrencyValueDecimal string `json:"currencyValueDecimal"`
}
type propsJson struct {
	CurrencySymbol string `json:"currencySymbol"`
}

const (
	// MaxVolumeThreshold is the maximum volume allowed for a transaction
	// TODO this is a placeholder value, while we define the actual threshold rules
	MaxVolumeThreshold = 1000000000000000
)

// DoCleanup processes the raw transactions to clean data and detect outliers.
// It returns a cleaned MicroBatch and a channel emitting Outliers.
func DoCleanup(batch io.MicroBatch, outlierChan chan Outlier) (MicroBatch, error) {
	var cleanedTransactions []Transaction

	// Process the raw transactions
	for _, transaction := range batch.Data {
		parsedTime, err := time.Parse("2006-01-02 15:04:05", transaction.Timestamp)
		if err != nil {
			outlierChan <- Outlier{
				RawTransaction: transaction,
				Reason:         fmt.Sprintf("invalid timestamp format: %v", err),
			}
			continue
		}

		// Parse the `Nums` field, which is a JSON string
		var numsJSON numsJson
		err = json.Unmarshal([]byte(transaction.Nums), &numsJSON)
		if err != nil {
			outlierChan <- Outlier{
				RawTransaction: transaction,
				Reason:         fmt.Sprintf("invalid JSON format in Nums field: %v", err),
			}
			continue
		}

		// Parse the `Props` field, which is a JSON string
		var propsJSON propsJson
		err = json.Unmarshal([]byte(transaction.Props), &propsJSON)
		if err != nil {
			outlierChan <- Outlier{
				RawTransaction: transaction,
				Reason:         fmt.Sprintf("invalid JSON format in Props field: %v", err),
			}
			continue
		}

		currencyValueDecimal, err := strconv.ParseFloat(numsJSON.CurrencyValueDecimal, 64)
		if err != nil {
			outlierChan <- Outlier{
				RawTransaction: transaction,
				Reason:         fmt.Sprintf("invalid volume format: %v", err),
			}
			continue
		}

		// Check for outliers (you can define your own criteria for outliers)
		if currencyValueDecimal < 0 || currencyValueDecimal > MaxVolumeThreshold {
			outlierChan <- Outlier{
				RawTransaction: transaction,
				Reason:         fmt.Sprintf("volume over threshold: %f", currencyValueDecimal),
			}
			continue
		}

		// TODO add more outlier detection rules here!

		// If everything is valid, add the transaction to the cleaned list
		cleanedTransactions = append(cleanedTransactions, Transaction{
			Timestamp:      parsedTime,
			ProjectID:      transaction.ProjectID,
			CurrencySymbol: propsJSON.CurrencySymbol,
			Volume:         currencyValueDecimal,
		})
	}

	return MicroBatch{Data: cleanedTransactions}, nil
}
