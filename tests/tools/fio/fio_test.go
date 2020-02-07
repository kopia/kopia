package fio

import (
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
			Name: "write-10g",
			Options: map[string]string{
				"size":    "1g",
				"nrfiles": "10",
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
				Name: "write-10g",
				Options: map[string]string{
					"size":    "1g",
					"nrfiles": "10",
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
