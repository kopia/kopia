package caching

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/kopia/kopia/internal/storagetesting"
	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/logging"

	"testing"
)

type tracer []string

func (tr *tracer) Printf(format string, args ...interface{}) {
	*tr = append(*tr, fmt.Sprintf(format, args...))
	log.Printf("base: %v", fmt.Sprintf(format, args...))
}

func (tr *tracer) clear() {
	*tr = nil
}

func (tr *tracer) assertActivityAndClear(t *testing.T, expectedLogs ...string) bool {
	b := tr.assertActivity(t, expectedLogs...)
	tr.clear()
	return b
}

func (tr *tracer) assertActivity(t *testing.T, expectedLogs ...string) bool {
	if !tr.matches(expectedLogs...) {
		t.Errorf("Unexpected calls. Got %v calls:\n%v\nExpected %v calls:\n%v\n",
			len(*tr),
			strings.Join(*tr, "\n"),
			len(expectedLogs),
			strings.Join(expectedLogs, "\n"))
		return false
	}

	return true
}

func (tr *tracer) matches(expectedLogs ...string) bool {
	if len(expectedLogs) != len(*tr) {
		return false
	}

	for i, entry := range *tr {
		if !strings.HasPrefix(entry, expectedLogs[i]) {
			return false
		}
	}

	return true
}

func (tr *tracer) assertNoActivity(t *testing.T) bool {
	return tr.assertActivity(t)
}

func TestCache(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "kopia-cache")
	if err != nil {
		t.Errorf("cannot create temp directory for testing: %v", err)
		return
	}
	defer os.RemoveAll(tmpdir)

	// tmpdir = "/tmp/cache"
	// os.RemoveAll(tmpdir)

	masterData := map[string][]byte{}
	master := storagetesting.NewMapStorage(masterData)

	var tr tracer
	master = logging.NewWrapper(master, logging.Output(tr.Printf))

	cache, err := NewWrapper(master, Options{CacheDir: tmpdir})
	defer cache.Close()
	if err != nil {
		t.Errorf("cannot create cache: %v", err)
		return
	}

	data1 := []byte("foo-bar")
	data2 := []byte("baz-qux")
	masterData["x"] = data1
	masterData["z"] = data2

	tr.assertNoActivity(t)

	storagetesting.AssertGetBlock(t, cache, "x", data1)
	tr.assertActivityAndClear(t, "GetBlock")
	storagetesting.AssertBlockExists(t, cache, "x", true)
	tr.assertNoActivity(t)

	storagetesting.AssertGetBlockNotFound(t, cache, "y")
	tr.assertActivityAndClear(t, "GetBlock")
	storagetesting.AssertBlockExists(t, cache, "y", false)
	tr.assertNoActivity(t)

	storagetesting.AssertGetBlock(t, cache, "z", data2)
	tr.assertActivityAndClear(t, "GetBlock")
	storagetesting.AssertBlockExists(t, cache, "z", true)
	tr.assertNoActivity(t)

	storagetesting.AssertGetBlock(t, cache, "x", data1)
	storagetesting.AssertBlockExists(t, cache, "x", true)
	tr.assertNoActivity(t)

	storagetesting.AssertGetBlockNotFound(t, cache, "y")
	storagetesting.AssertBlockExists(t, cache, "y", false)
	tr.assertNoActivity(t)

	storagetesting.AssertGetBlock(t, cache, "z", data2)
	storagetesting.AssertBlockExists(t, cache, "z", true)
	tr.assertNoActivity(t)

	cache.DeleteBlock("z")
	tr.assertActivityAndClear(t, "DeleteBlock")

	storagetesting.AssertBlockExists(t, cache, "z", false)
	storagetesting.AssertGetBlockNotFound(t, cache, "z")
	tr.assertNoActivity(t)

	cache.PutBlock("z", data1, storage.PutOptionsDefault)
	tr.assertActivityAndClear(t, "PutBlock")

	storagetesting.AssertBlockExists(t, cache, "z", true)
	tr.assertActivityAndClear(t, "BlockExists")
	storagetesting.AssertGetBlock(t, cache, "z", data1)
	tr.assertActivityAndClear(t, "GetBlock")

	cache.PutBlock("x2", data1, storage.PutOptionsDefault)
	tr.assertActivityAndClear(t, "PutBlock")

	storagetesting.AssertListResults(t, cache, "", "x", "x2", "z")
	tr.assertActivityAndClear(t, "ListBlocks")

	cache.Close()
	tr.assertActivityAndClear(t, "Close")
}
