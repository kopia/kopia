package logging_test

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/logging"
)

func TestLoggingStorage(t *testing.T) {
	outputCount := new(int32)

	myPrefix := "myprefix"
	myOutput := func(msg string, args ...interface{}) {
		msg = fmt.Sprintf(msg, args...)

		if !strings.HasPrefix(msg, myPrefix) {
			t.Errorf("unexpected prefix %v", msg)
		}

		atomic.AddInt32(outputCount, 1)
	}

	data := blobtesting.DataMap{}
	kt := map[blob.ID]time.Time{}
	underlying := blobtesting.NewMapStorage(data, kt, nil)

	st := logging.NewWrapper(underlying, testlogging.Printf(myOutput, ""), myPrefix)
	if st == nil {
		t.Fatalf("unexpected result: %v", st)
	}

	ctx := testlogging.Context(t)
	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})

	if err := st.Close(ctx); err != nil {
		t.Fatalf("err: %v", err)
	}

	if *outputCount == 0 {
		t.Errorf("did not write any output!")
	}

	if got, want := st.ConnectionInfo().Type, underlying.ConnectionInfo().Type; got != want {
		t.Errorf("unexpected connection infor %v, want %v", got, want)
	}
}
