package testutil_test

import (
	"testing"

	"github.com/kopia/kopia/internal/testutil"
)

// different ways a test can fail.
var cases = map[string]func(r *testutil.RetriableT){
	"Fail":    func(r *testutil.RetriableT) { r.Fail() },
	"FailNow": func(r *testutil.RetriableT) { r.FailNow() },
	"Error":   func(r *testutil.RetriableT) { r.Error("e") },
	"Errorf":  func(r *testutil.RetriableT) { r.Errorf("e") },
	"Fatal":   func(r *testutil.RetriableT) { r.Fatal("e") },
	"Fatalf":  func(r *testutil.RetriableT) { r.Fatalf("e") },
}

func TestRetriableSucceedsAfterRetry(t *testing.T) {
	for funName, failFun := range cases {
		failFun := failFun
		count := 0

		t.Run(funName, func(t *testing.T) {
			t.Parallel()
			testutil.Retry(t, func(t *testutil.RetriableT) {
				count++
				if count == 3 {
					return
				}

				failFun(t)
			})
			if count != 3 {
				t.Fatalf("expected loop to run 3 times")
			}
		})
	}
}
