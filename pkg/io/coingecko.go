package io

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
)

// Coin represents the cryptocurrency with its price in USD
type Coin struct {
	ID     string  `json:"id" csv:"ID"`
	Symbol string  `json:"symbol" csv:"Symbol"`
	Name   string  `json:"name" csv:"Name"`
	Price  float64 `json:"current_price" csv:"Price (USD)"`
}

// Currency2Values In-memory holder for currency values
type Currency2Values map[string]float64

// CoinGeckoClient represents the client that interacts with the CoinGecko API
type CoinGeckoClient struct {
	APIKey string
	Client *http.Client
}

const (
	throttlingWaitTime = 31 * time.Second // the Demo API has a rate limit of 30 requests per minute
	perPage            = 250
	apiBaseURL         = "https://api.coingecko.com/api/v3"
	httpTimeout        = 10 * time.Second
)

// NewCoinGeckoClient creates a new client and verifies connection with ping
func NewCoinGeckoClient(apiKey string) (*CoinGeckoClient, error) {
	client := &CoinGeckoClient{
		APIKey: apiKey,
		Client: &http.Client{
			Timeout: httpTimeout,
		},
	}

	if err := client.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to CoinGecko API: %w", err)
	}
	return client, nil
}

// Ping method checks if CoinGecko API is reachable
func (c *CoinGeckoClient) Ping() error {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/ping", apiBaseURL), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	addHeaders(req, c.APIKey)

	res, err := c.Client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to reach CoinGecko API: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to connect to CoinGecko API: %w", err)
	}

	return nil
}

// FetchCryptoData fetches all available cryptocurrencies with their current USD prices
func (c *CoinGeckoClient) FetchCryptoData(ctx context.Context) ([]Coin, error) {
	var allCoins []Coin
	page := 1

	for {
		url := fmt.Sprintf("%s/coins/markets?vs_currency=usd&per_page=%d&page=%d", apiBaseURL, perPage, page)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		addHeaders(req, c.APIKey)

		res, err := c.Client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch crypto data: %w", err)
		}
		defer res.Body.Close()

		if res.StatusCode == http.StatusUnauthorized {
			return nil, errors.New("unauthorized access to CoinGecko API")
		}

		// Handle "Too Many Requests" by waiting and retrying
		if res.StatusCode == http.StatusTooManyRequests {
			log.Printf("Too many requests. Waiting for %v before retrying, already fetched %d", throttlingWaitTime, len(allCoins))
			time.Sleep(throttlingWaitTime)
			continue
		}

		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("failed to fetch cryptocurrency data: status code %d", res.StatusCode)
		}

		var coins []Coin
		err = json.NewDecoder(res.Body).Decode(&coins)
		if err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		allCoins = append(allCoins, coins...)
		if len(coins) < perPage {
			break
		}

		// Move to the next page
		page++
	}

	return allCoins, nil
}

// Save cryptocurrency data as CSV using the provided io.Writer
func (c *CoinGeckoClient) Save(w io.Writer) error {
	coins, err := c.FetchCryptoData(context.Background()) // Add context to allow cancellation
	if err != nil {
		return fmt.Errorf("failed to fetch crypto data: %w", err)
	}

	log.Printf("Fetched %d cryptocurrencies\n", len(coins))
	if err := gocsv.Marshal(&coins, w); err != nil {
		return fmt.Errorf("failed to write CSV: %w", err)
	}

	return nil
}

// ReadCurrencyValues reads the currency values from the provided io.Reader
func ReadCurrencyValues(reader io.Reader) (Currency2Values, error) {
	var coins []Coin

	if err := gocsv.Unmarshal(reader, &coins); err != nil {
		return nil, fmt.Errorf("failed to unmarshal CSV: %w", err)
	}

	currencyMap := make(Currency2Values)
	for _, coin := range coins {
		currencyMap[strings.ToUpper(coin.Symbol)] = coin.Price
	}

	return currencyMap, nil
}

func addHeaders(req *http.Request, apiKey string) {
	req.Header.Add("accept", "application/json")
	req.Header.Add("x-cg-api-key", apiKey)
}
