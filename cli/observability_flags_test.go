package cli_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestMetricsPushFlags(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	mux := http.NewServeMux()

	var (
		mu     sync.Mutex
		urls   []string
		bodies []string
		auths  []string
	)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		urls = append(urls, r.RequestURI)
		d, _ := io.ReadAll(r.Body)
		bodies = append(bodies, r.Method+":"+string(d))
		u, p, _ := r.BasicAuth()

		auths = append(auths, u+":"+p)
	})

	tmp1 := testutil.TempDirectory(t)

	server := httptest.NewServer(mux)
	defer server.Close()

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", tmp1,
		"--metrics-push-addr="+server.URL,
		"--metrics-push-interval=30s",
		"--metrics-push-format=text",
	)

	env.RunAndExpectSuccess(t, "repo", "status",
		"--metrics-push-addr="+server.URL,
		"--metrics-push-interval=30s",
		"--metrics-push-grouping=a:b",
		"--metrics-push-username=user1",
		"--metrics-push-password=pass1",
		"--metrics-push-format=proto-text",
	)

	require.Equal(t, []string{
		"/metrics/job/kopia",     // initial
		"/metrics/job/kopia",     // final
		"/metrics/job/kopia/a/b", // initial
		"/metrics/job/kopia/a/b", // final
	}, urls)

	require.Equal(t, "user1:pass1", auths[len(auths)-1])

	for _, b := range bodies {
		// make sure bodies include some kopia metrics, don't need more
		require.Contains(t, b, "kopia_cache_hit_bytes_total")
	}

	env.RunAndExpectFailure(t, "repo", "status",
		"--metrics-push-addr="+server.URL,
		"--metrics-push-grouping=a=s",
	)
}

func TestOTLPFlags(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	// deprecated flag
	env.RunAndExpectFailure(t, "benchmark", "crypto", "--repeat=1", "--block-size=1KB", "--print-options", "--enable-jaeger-collector")

	// this has no effect whether OTLP collector is running or not.
	env.RunAndExpectSuccess(t, "benchmark", "crypto", "--repeat=1", "--block-size=1KB", "--print-options", "--otlp-trace")
}

func TestMetricsSaveToOutputDirFlags(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	tmp1 := testutil.TempDirectory(t)

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", tmp1)

	tmp2 := testutil.TempDirectory(t)

	env.RunAndExpectSuccess(t, "repo", "status", "--metrics-directory", tmp2)

	entries, err := os.ReadDir(tmp2)
	require.NoError(t, err)
	require.Len(t, entries, 1, "a metrics output file should have been created")
}
