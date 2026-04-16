package repo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/content"
)

func TestLocalConfig_withCaching(t *testing.T) {
	td := testutil.TempDirectory(t)

	originalLC := &LocalConfig{
		Caching: &content.CachingOptions{
			CacheDirectory: filepath.Join(td, "cache-dir"),
		},
	}

	cfgFile := filepath.Join(td, "repository.config")
	require.NoError(t, originalLC.writeToFile(cfgFile))

	rawLC := LocalConfig{}
	mustParseJSONFile(t, cfgFile, &rawLC)

	loadedLC, err := LoadConfigFromFile(cfgFile)
	require.NoError(t, err)

	if ospath.IsAbs(rawLC.Caching.CacheDirectory) {
		t.Fatalf("cache directory must be stored relative, was %v", rawLC.Caching.CacheDirectory)
	}

	if got, want := loadedLC.Caching.CacheDirectory, originalLC.Caching.CacheDirectory; got != want {
		t.Fatalf("cache directory did not round trip: %v, want %v", got, want)
	}
}

func TestLocalConfig_noCaching(t *testing.T) {
	td := testutil.TempDirectory(t)

	originalLC := &LocalConfig{}

	cfgFile := filepath.Join(td, "repository.config")
	require.NoError(t, originalLC.writeToFile(cfgFile))

	rawLC := LocalConfig{}
	mustParseJSONFile(t, cfgFile, &rawLC)

	loadedLC, err := LoadConfigFromFile(cfgFile)
	require.NoError(t, err)

	if got, want := loadedLC.Caching, originalLC.Caching; got != want {
		t.Fatalf("caching did not round trip: %v, want %v", got, want)
	}
}

func TestLocalConfig_notFound(t *testing.T) {
	if _, err := LoadConfigFromFile("nosuchfile.json"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected error %v: wanted ErrNotExist", err)
	}
}

func mustParseJSONFile(t *testing.T, fname string, o any) {
	t.Helper()

	f, err := os.Open(fname)
	require.NoError(t, err)

	defer f.Close()

	require.NoError(t, json.NewDecoder(f).Decode(o))
}
