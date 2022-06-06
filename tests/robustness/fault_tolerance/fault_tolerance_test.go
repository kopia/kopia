//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package robustness

import (
	"testing"

	"github.com/kopia/kopia/tests/robustness/engine"
)

var eng *engine.Engine // for use in the test functions

func TestMain(m *testing.M) {

	// assumptions: k10, kopia source code is available
	// connect to existing v1 kopia repo in S3
	// restore a snapshot to CF volume
	// connect/create another kopia repo in S3 - system under test
	// take snapshot of CF volume
	// perform changes to CF volume data, snapshot again - repeat multiple times
	// list blobs in SUT,
	// create a copy of v1 repo - can use the existing one, where to create the copy?
	//
	// delete random blobs - decide the number, start with 2

}
