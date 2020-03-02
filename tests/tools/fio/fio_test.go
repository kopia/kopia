package fio

import (
	"os"
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestFIORun(t *testing.T) {
	r, err := NewRunner()
	testenv.AssertNoError(t, err)

	defer r.Cleanup()

	stdout, stderr, err := r.Run()
	if err == nil {
		t.Fatal("Expected error to be set as no params were passed")
	}

	if !strings.Contains(stderr, "No job") {
		t.Fatal("Expected an error indicating no jobs were defined")
	}

	if !strings.Contains(stdout, "Print this page") {
		// Indicates the --help page has been printed
		t.Fatal("Expected --help page when running fio with no args")
	}
}

func TestFIORunConfig(t *testing.T) {
	r, err := NewRunner()
	testenv.AssertNoError(t, err)

	defer r.Cleanup()

	cfg := Config{
		{
			Name: "write-1m",
			Options: map[string]string{
				"size":      "1m",
				"blocksize": "4k",
				"nrfiles":   "10",
			},
		},
	}
	stdout, stderr, err := r.RunConfigs(cfg)
	testenv.AssertNoError(t, err)

	if stderr != "" {
		t.Error("Stderr was not empty")
	}

	if !strings.Contains(stdout, "rw=write") {
		t.Error("Expected the output to indicate writes took place")
	}
}

func TestFIOGlobalConfigOverride(t *testing.T) {
	r, err := NewRunner()
	testenv.AssertNoError(t, err)

	defer r.Cleanup()

	cfgs := []Config{
		{
			{
				Name: "global",
				Options: map[string]string{
					"rw": "read",
				},
			},
			{
				Name: "write-1m",
				Options: map[string]string{
					"size":      "1m",
					"blocksize": "4k",
					"nrfiles":   "10",
				},
			},
		},
	}
	stdout, _, err := r.RunConfigs(cfgs...)
	testenv.AssertNoError(t, err)

	if !strings.Contains(stdout, "rw=read") {
		t.Fatal("Expected the global config 'rw' flag to be overwritten by the passed config")
	}
}

func TestFIODockerRunner(t *testing.T) {
	if os.Getenv(FioDockerImageEnvKey) == "" {
		t.Skip("Test requires docker image env variable to be set", FioDockerImageEnvKey)
	}

	// Unset FIO_EXE for duration of test
	prevExeEnv := os.Getenv(FioExeEnvKey)
	defer os.Setenv(FioExeEnvKey, prevExeEnv) //nolint:errcheck

	err := os.Unsetenv(FioExeEnvKey)
	testenv.AssertNoError(t, err)

	r, err := NewRunner()
	testenv.AssertNoError(t, err)

	defer r.Cleanup() //nolint:errcheck

	cfgs := []Config{
		{
			{
				Name: "write-1m",
				Options: map[string]string{
					"blocksize": "4k",
					"size":      "1m",
					"nrfiles":   "10",
				},
			},
		},
	}

	_, _, err = r.RunConfigs(cfgs...)
	testenv.AssertNoError(t, err)
}
