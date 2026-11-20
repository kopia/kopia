package s3

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

const (
	fakeS3Bucket = "fake-bucket"

	noSuchKeyErrorXML = `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`

	quotaExceededErrorXML = `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>QuotaExceeded</Code><Message>Quota exceeded.</Message></Error>`
)

// fakeS3Server is a minimal S3 endpoint for exercising PutBlob without a real object store.
type fakeS3Server struct {
	failPut      bool
	lastModified time.Time

	putCount  atomic.Int32
	statCount atomic.Int32

	// retainUntilDate is the raw object lock retain-until date received in the
	// last PutObject header or PutObjectRetention request body.
	retainUntilDate atomic.Pointer[string]
}

func (f *fakeS3Server) lastRetainUntilDate() string {
	if d := f.retainUntilDate.Load(); d != nil {
		return *d
	}

	return ""
}

func (f *fakeS3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	switch {
	case r.Method == http.MethodPut && r.URL.Query().Has("retention"):
		var retention struct {
			RetainUntilDate string `xml:"RetainUntilDate"`
		}

		if err := xml.Unmarshal(body, &retention); err != nil {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		f.retainUntilDate.Store(&retention.RetainUntilDate)

		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodPut:
		f.putCount.Add(1)

		if f.failPut {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, quotaExceededErrorXML)

			return
		}

		retainUntilDate := r.Header.Get("X-Amz-Object-Lock-Retain-Until-Date")
		f.retainUntilDate.Store(&retainUntilDate)

		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodHead:
		f.statCount.Add(1)

		if f.failPut {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Last-Modified", f.lastModified.UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)

	default:
		// GET of the storage config blob during initialization.
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, noSuchKeyErrorXML)
	}
}

func newFakeS3Storage(t *testing.T, f *fakeS3Server) *s3Storage {
	t.Helper()

	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)

	st, err := newStorageWithCredentials(testlogging.Context(t), credentials.NewStaticV4("fake-key", "fake-secret", ""), &Options{
		Endpoint:    u.Host,
		BucketName:  fakeS3Bucket,
		Region:      "fake-region-1",
		DoNotUseTLS: true,
	})
	require.NoError(t, err)

	return st
}

func TestPutBlob_GetModTimeReturnsPutObjectError(t *testing.T) {
	t.Parallel()

	f := &fakeS3Server{failPut: true}
	st := newFakeS3Storage(t, f)

	var modTime time.Time

	err := st.PutBlob(testlogging.Context(t), "some-blob", gather.FromSlice([]byte("data")), blob.PutOptions{
		GetModTime: &modTime,
	})

	require.Error(t, err)
	require.NotErrorIs(t, err, blob.ErrBlobNotFound)

	var er minio.ErrorResponse

	require.ErrorAs(t, err, &er)
	require.Equal(t, "QuotaExceeded", er.Code)
	require.Equal(t, http.StatusForbidden, er.StatusCode)

	require.Equal(t, int32(1), f.putCount.Load())
	require.Zero(t, f.statCount.Load(), "metadata must not be fetched after a failed upload")
	require.True(t, modTime.IsZero())
}

func TestPutBlob_GetModTime(t *testing.T) {
	t.Parallel()

	lastModified := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	f := &fakeS3Server{lastModified: lastModified}
	st := newFakeS3Storage(t, f)

	var modTime time.Time

	err := st.PutBlob(testlogging.Context(t), "some-blob", gather.FromSlice([]byte("data")), blob.PutOptions{
		GetModTime: &modTime,
	})

	require.NoError(t, err)
	require.Equal(t, int32(1), f.putCount.Load())
	require.Equal(t, int32(1), f.statCount.Load())
	require.Equal(t, lastModified, modTime)
}

// requireRetainUntilDate verifies that the retain-until date sent to the server
// has at most second precision and is not earlier than the requested retention
// period measured from any point in [before, after].
func requireRetainUntilDate(t *testing.T, raw string, before, after time.Time, period time.Duration) {
	t.Helper()

	require.NotEmpty(t, raw, "retain-until date was not sent")
	require.NotContains(t, raw, ".", "retain-until date must not have sub-second precision")
	require.True(t, strings.HasSuffix(raw, "Z"), "retain-until date must be in UTC: %q", raw)

	got, err := time.Parse(time.RFC3339, raw)
	require.NoError(t, err)
	require.Zero(t, got.Nanosecond())

	require.False(t, got.Before(before.Add(period)), "retain-until %v is earlier than %v", got, before.Add(period))
	require.False(t, got.After(after.Add(period+time.Second)), "retain-until %v is later than %v", got, after.Add(period+time.Second))
}

func TestPutBlob_RetainUntilDateSecondPrecision(t *testing.T) {
	t.Parallel()

	const period = 24*time.Hour + 500*time.Millisecond

	f := &fakeS3Server{}
	st := newFakeS3Storage(t, f)

	before := clock.Now()
	err := st.PutBlob(testlogging.Context(t), "some-blob", gather.FromSlice([]byte("data")), blob.PutOptions{
		RetentionMode:   blob.Governance,
		RetentionPeriod: period,
	})
	after := clock.Now()

	require.NoError(t, err)
	require.Equal(t, int32(1), f.putCount.Load())
	requireRetainUntilDate(t, f.lastRetainUntilDate(), before, after, period)
}

func TestExtendBlobRetention_RetainUntilDateSecondPrecision(t *testing.T) {
	t.Parallel()

	const period = 24*time.Hour + 500*time.Millisecond

	f := &fakeS3Server{}
	st := newFakeS3Storage(t, f)

	before := clock.Now()
	err := st.ExtendBlobRetention(testlogging.Context(t), "some-blob", blob.ExtendOptions{
		RetentionMode:   blob.Governance,
		RetentionPeriod: period,
	})
	after := clock.Now()

	require.NoError(t, err)
	requireRetainUntilDate(t, f.lastRetainUntilDate(), before, after, period)
}
