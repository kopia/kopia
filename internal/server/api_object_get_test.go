package server

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/uitask"
)

type testObjectReader struct {
	*bytes.Reader
}

func (r testObjectReader) Close() error {
	return nil
}

func (r testObjectReader) Length() int64 {
	return int64(r.Size())
}

func TestServeObjectDownloadCreatesRestoreTask(t *testing.T) {
	contents := bytes.Repeat([]byte("test data"), 1000)
	obj := testObjectReader{bytes.NewReader(contents)}
	recorder := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/download", nil)
	tasks := uitask.NewManager(false)

	serveObjectDownload(req.Context(), tasks, recorder, req, "sample.bin", time.Time{}, obj)

	result := recorder.Result()
	t.Cleanup(func() { require.NoError(t, result.Body.Close()) })

	got, err := io.ReadAll(result.Body)
	require.NoError(t, err)
	require.Equal(t, contents, got)
	require.Equal(t, 200, result.StatusCode)

	taskList := tasks.ListTasks()
	require.Len(t, taskList, 1)

	task := taskList[0]
	require.Equal(t, "Restore", task.Kind)
	require.Equal(t, "File: sample.bin", task.Description)
	require.Equal(t, uitask.StatusSuccess, task.Status)
	require.Equal(t, uitask.SimpleCounter(1), task.Counters["Restored Files"])
	require.Equal(t, uitask.BytesCounter(int64(len(contents))), task.Counters["Restored Bytes"])
}
