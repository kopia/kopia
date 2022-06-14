package virtualfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/testlogging"
)

const (
	testFileName = "stream-file"
)

func TestStreamingFile(t *testing.T) {
	// Create a temporary file with test data
	content := []byte("Temporary file content")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("error creating pipe file: %v", err)
	}

	if _, err = w.Write(content); err != nil {
		t.Fatalf("error writing to pipe file: %v", err)
	}

	w.Close()

	f := StreamingFileFromReader(testFileName, r)

	rootDir := NewStaticDirectory("root", []fs.Entry{f})

	e, err := rootDir.Child(testlogging.Context(t), testFileName)
	if err != nil {
		t.Fatalf("error getting child entry: %v", err)
	}

	if e.Name() != testFileName {
		t.Fatalf("did not get expected filename: (actual) %v != %v (expected)", e.Name(), testFileName)
	}

	entries, err := fs.GetAllEntries(testlogging.Context(t), rootDir)
	if err != nil {
		t.Fatalf("error getting dir entries %v", err)
	}

	if len(entries) == 0 {
		t.Errorf("expected directory with 1 entry, got %v", rootDir)
	}

	// Read and compare data
	reader, err := f.GetReader(testlogging.Context(t))
	if err != nil {
		t.Fatalf("error getting streaming file reader: %v", err)
	}

	result := make([]byte, len(content))

	if _, err = reader.Read(result); err != nil {
		t.Fatalf("error reading streaming file: %v", err)
	}

	if !reflect.DeepEqual(result, content) {
		t.Fatalf("did not get expected file content: (actual) %v != %v (expected)", result, content)
	}
}

func TestStreamingFileGetReader(t *testing.T) {
	// Create a temporary file with test data
	content := []byte("Temporary file content")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("error creating pipe file: %v", err)
	}

	if _, err = w.Write(content); err != nil {
		t.Fatalf("error writing to pipe file: %v", err)
	}

	w.Close()

	f := StreamingFileFromReader(testFileName, r)

	// Read and compare data
	reader, err := f.GetReader(testlogging.Context(t))
	if err != nil {
		t.Fatalf("error getting streaming file reader: %v", err)
	}

	result := make([]byte, len(content))

	if _, err = reader.Read(result); err != nil {
		t.Fatalf("error reading streaming file: %v", err)
	}

	if !reflect.DeepEqual(result, content) {
		t.Fatalf("did not get expected file content: (actual) %v != %v (expected)", result, content)
	}

	// Second call to GetReader must fail
	_, err = f.GetReader(testlogging.Context(t))
	if err == nil {
		t.Fatal("expected error, got none")
	}

	if !errors.Is(err, errReaderAlreadyUsed) {
		t.Fatalf("did not get expected error: (actual) %v != %v (expected)", err, errReaderAlreadyUsed)
	}
}

func TestStreamingDirectory(t *testing.T) {
	// Create a temporary file with test data
	content := []byte("Temporary file content")
	r := bytes.NewReader(content)

	f := StreamingFileFromReader(testFileName, r)

	rootDir := NewStreamingDirectory(
		"root",
		func(
			ctx context.Context,
			callback func(context.Context, fs.Entry) error,
		) error {
			return callback(ctx, f)
		},
	)

	entries, err := fs.GetAllEntries(testlogging.Context(t), rootDir)
	require.NoError(t, err)

	assert.Len(t, entries, 1)

	e := entries[0]
	require.Equal(t, e.Name(), testFileName)

	// Read and compare data
	reader, err := f.GetReader(testlogging.Context(t))
	require.NoError(t, err)

	result := make([]byte, len(content))

	_, err = reader.Read(result)
	require.NoError(t, err)

	assert.True(t, reflect.DeepEqual(result, content))
}

func TestStreamingDirectory_MultipleIterationsFails(t *testing.T) {
	// Create a temporary file with test data
	content := []byte("Temporary file content")
	r := bytes.NewReader(content)

	f := StreamingFileFromReader(testFileName, r)

	rootDir := NewStreamingDirectory(
		"root",
		func(
			ctx context.Context,
			callback func(context.Context, fs.Entry) error,
		) error {
			return callback(ctx, f)
		},
	)

	entries, err := fs.GetAllEntries(testlogging.Context(t), rootDir)
	require.NoError(t, err)

	assert.Len(t, entries, 1)

	_, err = fs.GetAllEntries(testlogging.Context(t), rootDir)
	assert.Error(t, err)
}

var errCallback = errors.New("callback error")

func TestStreamingDirectory_ReturnsCallbackError(t *testing.T) {
	// Create a temporary file with test data
	content := []byte("Temporary file content")
	r := bytes.NewReader(content)

	f := StreamingFileFromReader(testFileName, r)

	rootDir := NewStreamingDirectory(
		"root",
		func(
			ctx context.Context,
			callback func(context.Context, fs.Entry) error,
		) error {
			return callback(ctx, f)
		},
	)

	err := rootDir.IterateEntries(testlogging.Context(t), func(context.Context, fs.Entry) error {
		return errCallback
	})
	assert.ErrorIs(t, err, errCallback)
}

var errIteration = errors.New("iteration error")

func TestStreamingDirectory_ReturnsReadDirError(t *testing.T) {
	rootDir := NewStreamingDirectory(
		"root",
		func(
			ctx context.Context,
			callback func(context.Context, fs.Entry) error,
		) error {
			return errIteration
		},
	)

	err := rootDir.IterateEntries(testlogging.Context(t), func(context.Context, fs.Entry) error {
		return nil
	})
	assert.ErrorIs(t, err, errIteration)
}
