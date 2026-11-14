package s3

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

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
}

func (f *fakeS3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)

	switch r.Method {
	case http.MethodPut:
		f.putCount.Add(1)

		if f.failPut {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, quotaExceededErrorXML)

			return
		}

		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.WriteHeader(http.StatusOK)

	case http.MethodHead:
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
	require.True(t, lastModified.Equal(modTime), "got mod time %v, want %v", modTime, lastModified)
}
