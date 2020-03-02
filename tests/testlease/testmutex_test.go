package testlease_test

import (
	"testing"
	"time"

	"github.com/kopia/kopia/tests/testlease"
)

func TestLease(t *testing.T) {
	testlease.RunWithLease(t, "some-key3", 10*time.Minute, func() {
		time.Sleep(3 * time.Second)
	})
}
