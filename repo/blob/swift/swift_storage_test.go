package swift

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
)

const testContainer = "kopia-test"

type fakeSwiftObject struct {
	data    []byte
	modTime time.Time
}

type fakeSwiftServer struct {
	*httptest.Server

	mu      sync.Mutex
	objects map[string]fakeSwiftObject
	now     time.Time
}

func newFakeSwiftServer(t *testing.T) *fakeSwiftServer {
	t.Helper()

	f := &fakeSwiftServer{
		objects: map[string]fakeSwiftObject{},
		now:     time.Now().UTC().Truncate(time.Second),
	}

	f.Server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(f.Close)

	return f
}

func (f *fakeSwiftServer) options(prefix string) *Options {
	return &Options{
		ContainerName: testContainer,
		Prefix:        prefix,
		AuthURL:       f.URL + "/v3",
		Username:      "user",
		Password:      "password",
		DomainName:    "Default",
		TenantName:    "project",
		Region:        "RegionOne",
	}
}

func (f *fakeSwiftServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/v3/auth/tokens":
		f.serveToken(w, r)
	case r.URL.Path == "/v1/AUTH_test/"+testContainer:
		f.serveList(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/AUTH_test/"+testContainer+"/"):
		f.serveObject(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeSwiftServer) serveToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Subject-Token", "fake-token")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	fmt.Fprintf(w, `{
  "token": {
    "methods": ["password"],
    "expires_at": "2030-01-01T00:00:00.000000Z",
    "issued_at": "2026-05-05T12:00:00.000000Z",
    "user": {
      "id": "user-id",
      "name": "user",
      "domain": {"id": "default", "name": "Default"}
    },
    "project": {
      "id": "project-id",
      "name": "project",
      "domain": {"id": "default", "name": "Default"}
    },
    "catalog": [{
      "type": "object-store",
      "name": "swift",
      "endpoints": [{
        "interface": "public",
        "region": "RegionOne",
        "url": %q
      }]
    }]
  }
}`, f.URL+"/v1/AUTH_test")
}

func (f *fakeSwiftServer) serveList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	marker := r.URL.Query().Get("marker")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	f.mu.Lock()
	defer f.mu.Unlock()

	var names []string
	for name := range f.objects {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if marker != "" && name <= marker {
			continue
		}
		names = append(names, name)
	}

	sort.Strings(names)
	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}

	var items []map[string]any
	for _, name := range names {
		o := f.objects[name]
		items = append(items, map[string]any{
			"name":          name,
			"bytes":         int64(len(o.data)),
			"last_modified": o.modTime.Format("2006-01-02T15:04:05.000000"),
			"content_type":  "application/x-kopia",
			"hash":          "fake",
		})
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(items); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (f *fakeSwiftServer) serveObject(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/v1/AUTH_test/"+testContainer+"/")

	switch r.Method {
	case http.MethodPut:
		f.putObject(w, r, name)
	case http.MethodHead:
		f.headObject(w, name)
	case http.MethodGet:
		f.getObject(w, r, name)
	case http.MethodDelete:
		f.deleteObject(w, name)
	default:
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
	}
}

func (f *fakeSwiftServer) putObject(w http.ResponseWriter, r *http.Request, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if r.Header.Get("If-None-Match") == "*" {
		if _, ok := f.objects[name]; ok {
			http.Error(w, "already exists", http.StatusPreconditionFailed)
			return
		}
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	f.now = f.now.Add(time.Second)
	f.objects[name] = fakeSwiftObject{data: data, modTime: f.now}

	w.Header().Set("Last-Modified", f.now.Format(http.TimeFormat))
	w.WriteHeader(http.StatusCreated)
}

func (f *fakeSwiftServer) headObject(w http.ResponseWriter, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	o, ok := f.objects[name]
	if !ok {
		http.NotFound(w, nil)
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(o.data)))
	w.Header().Set("Last-Modified", o.modTime.Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeSwiftServer) getObject(w http.ResponseWriter, r *http.Request, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	o, ok := f.objects[name]
	if !ok {
		http.NotFound(w, nil)
		return
	}

	data := o.data
	if r.Header.Get("Range") != "" {
		var start, end int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if start < 0 || end < start || start >= len(data) {
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= len(data) {
			end = len(data) - 1
		}
		data = data[start : end+1]
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("Last-Modified", o.modTime.Format(http.TimeFormat))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("Last-Modified", o.modTime.Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
	}

	_, _ = w.Write(data)
}

func (f *fakeSwiftServer) deleteObject(w http.ResponseWriter, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.objects[name]; !ok {
		http.NotFound(w, nil)
		return
	}

	delete(f.objects, name)
	w.WriteHeader(http.StatusNoContent)
}

func TestSwiftStorage(t *testing.T) {
	ctx := testlogging.Context(t)
	fake := newFakeSwiftServer(t)

	st, err := New(ctx, fake.options("test-prefix/"), false)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, st.Close(context.Background()))
	})

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, swiftValidationOptions))
}

func TestPutBlobDoNotRecreate(t *testing.T) {
	ctx := testlogging.Context(t)
	fake := newFakeSwiftServer(t)

	st, err := New(ctx, fake.options(""), false)
	require.NoError(t, err)

	require.NoError(t, st.PutBlob(ctx, "same", gather.FromSlice([]byte("first")), blob.PutOptions{}))
	err = st.PutBlob(ctx, "same", gather.FromSlice([]byte("second")), blob.PutOptions{DoNotRecreate: true})
	require.ErrorIs(t, err, blob.ErrBlobAlreadyExists)

	var out gather.WriteBuffer
	defer out.Close()
	require.NoError(t, st.GetBlob(ctx, "same", 0, -1, &out))
	require.Equal(t, []byte("first"), out.ToByteSlice())
}

func TestPutBlobUnsupportedOptions(t *testing.T) {
	ctx := testlogging.Context(t)
	fake := newFakeSwiftServer(t)

	st, err := New(ctx, fake.options(""), false)
	require.NoError(t, err)

	err = st.PutBlob(ctx, "retained", gather.FromSlice([]byte("x")), blob.PutOptions{RetentionMode: blob.Governance, RetentionPeriod: time.Hour})
	require.ErrorIs(t, err, blob.ErrUnsupportedPutBlobOption)

	err = st.PutBlob(ctx, "mtime", gather.FromSlice([]byte("x")), blob.PutOptions{SetModTime: time.Now()})
	require.ErrorIs(t, err, blob.ErrSetTimeUnsupported)
}

func TestGetBlobZeroLength(t *testing.T) {
	ctx := testlogging.Context(t)
	fake := newFakeSwiftServer(t)

	st, err := New(ctx, fake.options(""), false)
	require.NoError(t, err)

	var out gather.WriteBuffer
	defer out.Close()

	require.ErrorIs(t, st.GetBlob(ctx, "missing", 0, 0, &out), blob.ErrBlobNotFound)

	require.NoError(t, st.PutBlob(ctx, "empty", gather.FromSlice(nil), blob.PutOptions{}))
	require.NoError(t, st.GetBlob(ctx, "empty", 0, 0, &out))
	require.Equal(t, 0, out.Length())

	require.NoError(t, st.PutBlob(ctx, "non-empty", gather.FromSlice([]byte("abc")), blob.PutOptions{}))
	require.NoError(t, st.GetBlob(ctx, "non-empty", 3, 0, &out))
	require.ErrorIs(t, st.GetBlob(ctx, "non-empty", 4, 0, &out), blob.ErrInvalidRange)
}

func TestReadOnlySwiftStorage(t *testing.T) {
	ctx := testlogging.Context(t)
	fake := newFakeSwiftServer(t)
	opt := fake.options("")
	opt.ReadOnly = true

	st, err := New(ctx, opt, false)
	require.NoError(t, err)
	require.True(t, st.IsReadOnly())
	require.ErrorIs(t, st.PutBlob(ctx, "x", gather.FromSlice([]byte("x")), blob.PutOptions{}), readonly.ErrReadonly)
}

func TestTranslateError(t *testing.T) {
	require.NoError(t, translateError(nil))
	require.ErrorIs(t, translateError(gophercloudStatusError(http.StatusUnauthorized)), blob.ErrInvalidCredentials)
	require.ErrorIs(t, translateError(gophercloudStatusError(http.StatusForbidden)), blob.ErrInvalidCredentials)
	require.ErrorIs(t, translateError(gophercloudStatusError(http.StatusNotFound)), blob.ErrBlobNotFound)
	require.ErrorIs(t, translateError(gophercloudStatusError(http.StatusRequestedRangeNotSatisfiable)), blob.ErrInvalidRange)
	require.ErrorIs(t, translateError(gophercloudStatusError(http.StatusPreconditionFailed)), blob.ErrBlobAlreadyExists)
}

func gophercloudStatusError(status int) error {
	return gophercloud.ErrUnexpectedResponseCode{Actual: status}
}

func getProviderOptions(t *testing.T) *Options {
	t.Helper()

	v := os.Getenv("KOPIA_SWIFT_CREDS")
	if v == "" {
		t.Skip("KOPIA_SWIFT_CREDS is not set")
	}

	var opt Options
	require.NoError(t, json.NewDecoder(strings.NewReader(v)).Decode(&opt))
	require.Empty(t, opt.Prefix, "KOPIA_SWIFT_CREDS must not specify a prefix")

	return &opt
}

func TestSwiftProviderTestCredentials(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	opt := getProviderOptions(t)
	ctx := testlogging.Context(t)

	st, err := New(ctx, opt, false)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, st.Close(testlogging.ContextForCleanup(t)))
	})

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

//nolint:gochecknoglobals
var swiftValidationOptions = providervalidation.Options{
	MaxClockDrift:                   3 * time.Minute,
	ConcurrencyTestDuration:         200 * time.Millisecond,
	NumEquivalentStorageConnections: 2,
	NumPutBlobWorkers:               1,
	NumGetBlobWorkers:               1,
	NumGetMetadataWorkers:           1,
	NumListBlobsWorkers:             1,
	MaxBlobLength:                   64 << 10,
}
