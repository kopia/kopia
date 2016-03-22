package cas

import (
	"bytes"
	"testing"
)

func TestBufferManager(t *testing.T) {
	mgr := newBufferManager(10)
	defer mgr.close()

	verifyBufferClean := func(b *bytes.Buffer) {
		if b.Cap() != 10 {
			t.Errorf("unexpected cap: %v", b.Cap())
		}
		if b.Len() != 0 {
			t.Errorf("unexpected len: %v", b.Len())
		}
	}

	b := mgr.newBuffer()
	if mgr.outstandingCount != 1 {
		t.Errorf("unexpected outstandingCount: %v", mgr.outstandingCount)
	}
	b1 := mgr.newBuffer()
	verifyBufferClean(b)
	verifyBufferClean(b1)
	if mgr.outstandingCount != 2 {
		t.Errorf("unexpected outstandingCount: %v", mgr.outstandingCount)
	}
	closer := mgr.returnBufferOnClose(b)
	closer.Close()
	if mgr.outstandingCount != 1 {
		t.Errorf("unexpected outstandingCount: %v", mgr.outstandingCount)
	}
	mgr.returnBuffer(b)
	if mgr.outstandingCount != 0 {
		t.Errorf("unexpected outstandingCount: %v", mgr.outstandingCount)
	}
	b2 := mgr.newBuffer()
	if mgr.outstandingCount != 1 {
		t.Errorf("unexpected outstandingCount: %v", mgr.outstandingCount)
	}
	verifyBufferClean(b2)
	mgr.returnBuffer(b2)
	if mgr.outstandingCount != 0 {
		t.Errorf("unexpected outstandingCount: %v", mgr.outstandingCount)
	}
}
