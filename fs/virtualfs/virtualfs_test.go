package virtualfs

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/kopia/kopia/fs"
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

	filename := "stream-file"
	f := StreamingFileFromReader(filename, r)

	rootDir := NewStaticDirectory("root", fs.Entries{f})

	e, err := rootDir.Child(context.TODO(), filename)
	if err != nil {
		t.Fatalf("error getting child entry: %v", err)
	}

	if e.Name() != filename {
		t.Fatalf("did not get expected filename: (actual) %v != %v (expected)", e.Name(), filename)
	}

	entries := fs.Entries(nil)

	err = rootDir.IterateEntries(context.TODO(), func(innerCtx context.Context, e fs.Entry) error {
		entries = append(entries, e)
		return nil
	})
	if err != nil {
		t.Fatalf("error getting dir entries %v", err)
	}

	if len(entries) == 0 {
		t.Errorf("expected directory with 1 entry, got %v", rootDir)
	}

	// Read and compare data
	reader, err := f.GetReader(context.TODO())
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

	filename := "stream-file"
	f := StreamingFileFromReader(filename, r)

	// Read and compare data
	reader, err := f.GetReader(context.TODO())
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
	_, err = f.GetReader(context.TODO())
	if err == nil {
		t.Fatal("expected error, got none")
	}

	if !errors.Is(err, errReaderAlreadyUsed) {
		t.Fatalf("did not get expected error: (actual) %v != %v (expected)", err, errReaderAlreadyUsed)
	}
}
