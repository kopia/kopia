//go:build darwin || (linux && amd64)

package framework

import (
	"context"
	"log"
	"sync"

	"github.com/kopia/kopia/tests/robustness"
)

// MultiClientFileWriter manages a set of client FileWriter instances and
// implements the FileWriter interface itself. FileWriter methods must be
// provided with a client-wrapped context so the MultiClientFileWriter can
// delegate to a specific client FileWriter.
type MultiClientFileWriter struct {
	// Map of client ID to FileWriter and associated mutex
	mu sync.RWMutex

	// +checklocks:mu
	fileWriters map[string]FileWriter

	// Function used to generate new FileWriters
	newFileWriter newFileWriterFn
}

// MultiClientFileWriter implements robustness.FileWriter.
var _ robustness.FileWriter = (*MultiClientFileWriter)(nil)

type newFileWriterFn func() (FileWriter, error)

// NewMultiClientFileWriter returns a MultiClientFileWriter that is responsible
// for delegating FileWriter method calls to a specific client's FileWriter instance.
func NewMultiClientFileWriter(f newFileWriterFn) *MultiClientFileWriter {
	return &MultiClientFileWriter{
		newFileWriter: f,
		fileWriters:   map[string]FileWriter{},
	}
}

// DataDirectory delegates to a specific client's FileWriter.
func (mcfw *MultiClientFileWriter) DataDirectory(ctx context.Context) string {
	fw, err := mcfw.createOrGetFileWriter(ctx)
	if err != nil {
		return ""
	}

	return fw.DataDirectory(ctx)
}

// WriteRandomFiles delegates to a specific client's FileWriter.
func (mcfw *MultiClientFileWriter) WriteRandomFiles(ctx context.Context, opts map[string]string) (map[string]string, error) {
	fw, err := mcfw.createOrGetFileWriter(ctx)
	if err != nil {
		return opts, err
	}

	return fw.WriteRandomFiles(ctx, opts)
}

// DeleteRandomSubdirectory delegates to a specific client's FileWriter.
func (mcfw *MultiClientFileWriter) DeleteRandomSubdirectory(ctx context.Context, opts map[string]string) (map[string]string, error) {
	fw, err := mcfw.createOrGetFileWriter(ctx)
	if err != nil {
		return opts, err
	}

	return fw.DeleteRandomSubdirectory(ctx, opts)
}

// DeleteDirectoryContents delegates to a specific client's FileWriter.
func (mcfw *MultiClientFileWriter) DeleteDirectoryContents(ctx context.Context, opts map[string]string) (map[string]string, error) {
	fw, err := mcfw.createOrGetFileWriter(ctx)
	if err != nil {
		return opts, err
	}

	return fw.DeleteDirectoryContents(ctx, opts)
}

// DeleteEverything delegates to a specific client's FileWriter.
func (mcfw *MultiClientFileWriter) DeleteEverything(ctx context.Context) error {
	fw, err := mcfw.createOrGetFileWriter(ctx)
	if err != nil {
		return err
	}

	return fw.DeleteEverything(ctx)
}

// Cleanup delegates to a specific client's FileWriter for cleanup and removes
// the client FileWriter instance from the MultiClientFileWriter.
func (mcfw *MultiClientFileWriter) Cleanup() {
	mcfw.mu.Lock()
	defer mcfw.mu.Unlock()

	for clientID, fw := range mcfw.fileWriters {
		fw.Cleanup()
		delete(mcfw.fileWriters, clientID)
	}
}

// createOrGetFileWriter gets a client's FileWriter from the given context if
// possible or creates a new FileWriter.
func (mcfw *MultiClientFileWriter) createOrGetFileWriter(ctx context.Context) (robustness.FileWriter, error) {
	c := UnwrapContext(ctx)
	if c == nil {
		log.Println("Context does not contain a client")
		return nil, robustness.ErrKeyNotFound
	}

	// Get existing FileWriter if available
	mcfw.mu.RLock()
	fw, ok := mcfw.fileWriters[c.ID]
	mcfw.mu.RUnlock()

	if ok {
		return fw, nil
	}

	// Create new FileWriter and register with MultiClientFileWriter
	fw, err := mcfw.newFileWriter()
	if err != nil {
		return nil, err
	}

	mcfw.mu.Lock()
	defer mcfw.mu.Unlock()

	mcfw.fileWriters[c.ID] = fw

	return fw, nil
}
