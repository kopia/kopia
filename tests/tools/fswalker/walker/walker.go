//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package walker wraps calls to the fswalker Walker
package walker

import (
	"context"
	"os"

	"github.com/google/fswalker"
	fspb "github.com/google/fswalker/proto/fswalker"

	"github.com/kopia/kopia/tests/tools/fswalker/protofile"
)

const (
	// MaxFileSizeToHash gives an upper bound to the size of file that can be hashed by the walker.
	MaxFileSizeToHash = 1 << 32
)

// Walk performs a walk governed by the contents of the provided
// Policy, and returns the pointer to the Walk.
func Walk(ctx context.Context, policy *fspb.Policy) (*fspb.Walk, error) { //nolint:interfacer
	f, err := os.CreateTemp("", "fswalker-policy-")
	if err != nil {
		return nil, err
	}

	f.Close() //nolint:errcheck

	policyFileName := f.Name()
	defer os.RemoveAll(policyFileName) //nolint:errcheck

	err = protofile.WriteTextProto(policyFileName, policy)
	if err != nil {
		return nil, err
	}

	walker, err := fswalker.WalkerFromPolicyFile(ctx, policyFileName)
	if err != nil {
		return nil, err
	}

	var retWalk *fspb.Walk

	walker.WalkCallback = func(ctx context.Context, walk *fspb.Walk) error {
		retWalk = walk
		return nil
	}

	err = walker.Run(ctx)
	if err != nil {
		return nil, err
	}

	return retWalk, nil
}

// WalkPathHash performs a walk at the path prvided and returns a pointer
// to the Walk result.
func WalkPathHash(ctx context.Context, path string) (*fspb.Walk, error) {
	return Walk(ctx, &fspb.Policy{
		Version:         1,
		Include:         []string{path},
		HashPfx:         []string{""}, // Hash everything
		MaxHashFileSize: MaxFileSizeToHash,
		WalkCrossDevice: true,
	})
}
