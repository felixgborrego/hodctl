package sink

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"hodctl/pkg/worker"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

// BigQuerySink handles streaming inserts to BigQuery for aggregation results.
type BigQuerySink struct {
	client    *bigquery.Client
	datasetID string
	tableID   string
	ctx       context.Context
}

// Constants for schema fields
const (
	fieldDate                 = "Date"
	fieldProjectID            = "ProjectId"
	fieldNumberOfTransactions = "NumberOfTransactions"
	fieldTotalVolumeUsd       = "TotalVolumeUsd"
)

// Define the schema for the new table
var schema = bigquery.Schema{
	{Name: fieldDate, Type: bigquery.DateFieldType, Required: true},
	{Name: fieldProjectID, Type: bigquery.StringFieldType, Required: true},
	{Name: fieldNumberOfTransactions, Type: bigquery.IntegerFieldType, Required: true},
	{Name: fieldTotalVolumeUsd, Type: bigquery.FloatFieldType, Required: true},
}

var tableMetadata = bigquery.TableMetadata{
	Schema: schema,
	TimePartitioning: &bigquery.TimePartitioning{
		Field: fieldDate, // Partition by Date field
	},
	Clustering: &bigquery.Clustering{
		Fields: []string{fieldProjectID}, // Cluster by ProjectId
	},
}

// NewBigQuerySinkFromPath creates a new BigQuery sink from a URI in the format bq://projectid/datasetid/tableid.
func NewBigQuerySinkFromPath(ctx context.Context, uri string) (*BigQuerySink, error) {
	projectID, datasetID, tableID, err := parseBigQueryURI(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse BigQuery URI: %w", err)
	}
	return NewBigQuerySink(ctx, projectID, datasetID, tableID)
}

// NewBigQuerySink creates a new BigQuery sink with the specified project, dataset, and table.
func NewBigQuerySink(ctx context.Context, projectID, datasetID, tableID string) (*BigQuerySink, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	// Ensure the table exists or create it
	if err := ensureTableExists(ctx, client, datasetID, tableID); err != nil {
		return nil, fmt.Errorf("failed to ensure table exists: %w", err)
	}

	return &BigQuerySink{
		client:    client,
		datasetID: datasetID,
		tableID:   tableID,
		ctx:       ctx,
	}, nil
}

// ensureTableExists checks if the table exists and creates it if it doesn't.
func ensureTableExists(ctx context.Context, client *bigquery.Client, datasetID, tableID string) error {
	table := client.Dataset(datasetID).Table(tableID)

	_, err := table.Metadata(ctx)
	if err == nil {
		// Table exists, nothing to do
		return nil
	}

	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == 404 {
		// Table does not exist, create it
		fmt.Printf("Table %s does not exist, creating table...\n", tableID)

		// Create the table with partitioning and clustering
		return table.Create(ctx, &tableMetadata)
	}

	return fmt.Errorf("failed to get table metadata: %w", err)
}

// WriteAgg writes the aggregated data into BigQuery using streaming inserts.
func (s *BigQuerySink) WriteAgg(aggs []worker.Agg) (int, error) {
	inserter := s.client.Dataset(s.datasetID).Table(s.tableID).Inserter()

	// Convert worker.Agg to BigQuery rows
	rows := make([]*bigquery.ValuesSaver, len(aggs))
	for i, agg := range aggs {
		rows[i] = aggToRow(agg)
	}

	// Stream the data to BigQuery
	if err := inserter.Put(s.ctx, rows); err != nil {
		return 0, fmt.Errorf("failed to insert data into BigQuery: %w", err)
	}

	return len(aggs), nil
}

// Close closes the BigQuery client.
func (s *BigQuerySink) Close() error {
	return s.client.Close()
}

// aggToRow converts a worker.Agg to a BigQuery row (ValuesSaver).
func aggToRow(agg worker.Agg) *bigquery.ValuesSaver {
	return &bigquery.ValuesSaver{
		Schema: schema,
		Row: []bigquery.Value{
			agg.Date,
			agg.ProjectId,
			agg.NumberOfTransactions,
			agg.TotalVolumeUsd,
		},
	}
}

// parseBigQueryURI parses a URI of the form bq://projectid/datasetid/tableid.
func parseBigQueryURI(uri string) (string, string, string, error) {
	if !strings.HasPrefix(uri, "bq://") {
		return "", "", "", fmt.Errorf("URI must start with 'bq://'")
	}

	parsedURL, err := url.Parse(uri)
	if err != nil {
		return "", "", "", fmt.Errorf("invalid URI format: %w", err)
	}

	parts := strings.Split(strings.Trim(parsedURL.Path, "/"), "/")
	if len(parts) != 2 {
		return "", "", "", fmt.Errorf("URI must have the format 'bq://projectid/datasetid/tableid'")
	}

	return parsedURL.Host, parts[0], parts[1], nil
}
