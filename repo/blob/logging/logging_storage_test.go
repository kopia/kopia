package logging

import (
	"context"
	"strings"
	"testing"

	"github.com/kopia/kopia/internal/blobtesting"
)

func TestLoggingStorage(t *testing.T) {
	var outputCount int

	myPrefix := "myprefix"
	myOutput := func(msg string, args ...interface{}) {
		if !strings.HasPrefix(msg, myPrefix) {
			t.Errorf("unexpected prefix %v", msg)
		}
		outputCount++
	}

	data := blobtesting.DataMap{}
	underlying := blobtesting.NewMapStorage(data, nil, nil)

	st := NewWrapper(underlying, Output(myOutput), Prefix(myPrefix))
	if st == nil {
		t.Fatalf("unexpected result: %v", st)
	}

	ctx := context.Background()
	blobtesting.VerifyStorage(ctx, t, st)

	if err := st.Close(ctx); err != nil {
		t.Fatalf("err: %v", err)
	}

	if outputCount == 0 {
		t.Errorf("did not write any output!")
	}

	if got, want := st.ConnectionInfo().Type, underlying.ConnectionInfo().Type; got != want {
		t.Errorf("unexpected connection infor %v, want %v", got, want)
	}
}
