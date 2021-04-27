// +build darwin,amd64 linux,amd64

package multiclienttest

import (
	"context"
	"strconv"
	"testing"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
	"github.com/kopia/kopia/tests/testenv"
)

func TestExample(t *testing.T) {
	fileSize := 40 * 1024 * 1024
	numFiles := 1
	numClients := 4

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	// Define per-client test actions
	f := func(ctx context.Context) {
		_, err := eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
		testenv.AssertNoError(t, err)
	}

	// Create or obtain root context
	ctx := testlogging.Context(t)

	// Obtain a slice of derived contexts, each wrapped with a unique client
	// ctxs := framework.NewClientContexts(ctx, numClients)

	// Run test actions for each client concurrently
	th.RunN(ctx, numClients, f)
}
