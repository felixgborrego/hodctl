package sink

import (
	"fmt"
	"hodctl/pkg/io"
	"hodctl/pkg/worker"

	"github.com/gocarina/gocsv"
)

// VfsSink struct, which will handle writing to a virtual file system
type VfsAggSink struct {
	writer *io.VfsReaderWriter
}

type VfsOutlierSink struct {
	writer *io.VfsReaderWriter
}

// WriteAgg method implementation for VfsSink that writes Agg data in CSV format
func (s *VfsAggSink) WriteAgg(aggs []worker.Agg) (int, error) {
	err := gocsv.Marshal(&aggs, s.writer)
	if err != nil {
		return 0, fmt.Errorf("failed to write CSV: %w", err)
	}

	// Return the number of records written
	return len(aggs), nil
}

func (s *VfsOutlierSink) Close() error {
	return s.writer.Close()
}

func (s *VfsAggSink) Close() error {
	return s.writer.Close()
}

// WriteError method implementation for VfsSink that writes Outlier data in CSV format
func (s *VfsOutlierSink) WriteError(outliersCh <-chan worker.Outlier) error {
	writer := gocsv.DefaultCSVWriter(s.writer)
	interfaceChan := make(chan interface{})

	go func() {
		defer close(interfaceChan)
		for outlier := range outliersCh {
			interfaceChan <- outlier // Cast each outlier to interface{}
		}
	}()
	err := gocsv.MarshalChanWithoutHeaders(interfaceChan, writer)
	if err != nil {
		return err
	}

	if err != nil {
		return fmt.Errorf("failed to write CSV: %w", err)
	}

	if err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}
