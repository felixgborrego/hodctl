package sink

import (
	"context"
	"fmt"
	"hodctl/pkg/io"
	"hodctl/pkg/worker"
	"strings"
)

// AggSink for writing aggregated transactions to a sink.
type AggSink interface {
	WriteAgg([]worker.Agg) (int, error)
	Close() error
}

// ErrSink for writing errors to a sink.
type ErrSink interface {
	WriteError(outliers <-chan worker.Outlier) error
	Close() error
}

// NewAggSink initializes a new Sink based on the path.
// If the path starts with "bq", it returns a BQSink
// For any other path, it returns a VfsSink.
func NewAggSink(ctx context.Context, path string) (AggSink, error) {
	if strings.HasPrefix(path, "bq") {
		return NewBigQuerySinkFromPath(ctx, path)
	}

	// Create a VfsReaderWriter for the given path
	vfsWriter, err := io.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create VfsSink: %w", err)
	}

	return &VfsAggSink{
		writer: vfsWriter,
	}, nil
}

func NewOutlierSink(ctx context.Context, path string) (*VfsOutlierSink, error) {
	// Create a VfsReaderWriter for the given path
	vfsWriter, err := io.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create VfsSink: %w", err)
	}

	return &VfsOutlierSink{
		writer: vfsWriter,
	}, nil
}
