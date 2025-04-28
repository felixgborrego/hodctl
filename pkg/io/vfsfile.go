package io

import (
	"fmt"
	"github.com/c2fo/vfs/v6/utils"

	"github.com/c2fo/vfs/v6"
	"github.com/c2fo/vfs/v6/vfssimple"
)

// VfsReaderWriter handles reading and writing to a virtual file system.
type VfsReaderWriter struct {
	file vfs.File
	path string
}

// Open initializes a VfsReader for both GCS and local files.
func Open(path string) (*VfsReaderWriter, error) {

	path, err := utils.PathToURI(path)
	if err != nil {
		return nil, fmt.Errorf("failed to convert path to URI: %w", err)
	}

	file, err := vfssimple.NewFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return &VfsReaderWriter{
		file,
		path,
	}, nil
}

// Read implements the io.Reader interface by reading data from the Vfs file.
func (r *VfsReaderWriter) Read(p []byte) (int, error) {
	return r.file.Read(p)
}

// Write implements the io.Writer interface by writing data to the Vfs file.
func (r *VfsReaderWriter) Write(p []byte) (int, error) {
	return r.file.Write(p)
}

// Close closes the file after operations are done.
func (r *VfsReaderWriter) Close() error {
	return r.file.Close()
}
