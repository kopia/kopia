package repo_test

import (
	"testing"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/splitter"
)

const maxGRPCMessageOverhead = 1024

// TestMaxGRPCMessageSize ensures that MaxGRPCMessageSize is set to a value greater than all supported
// splitters + some safety margin.
func TestMaxGRPCMessageSize(t *testing.T) {
	var maxmax int

	for _, s := range splitter.SupportedAlgorithms() {
		if m := splitter.GetFactory(s)().MaxSegmentSize(); m > maxmax {
			maxmax = m
		}
	}

	if got, want := maxmax, repo.MaxGRPCMessageSize-maxGRPCMessageOverhead; got > want {
		t.Fatalf("invalid constant MaxGRPCMessageSize: %v, want >=%v", got, want)
	}
}
