// Package fio wraps calls to the fio tool.
// It assumes the tool is executable by "fio", but
// gives the option to specify another executable
// path by setting environment variable FIO_EXE.
package fio

import (
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// List of fio flags
const (
	JobNameFlag = "--name"
)

// Runner is a helper for running fio commands
type Runner struct {
	Exe     string
	DataDir string
	Global  Config
}

// NewRunner creates a new fio runner
func NewRunner() (*Runner, error) {
	Exe := os.Getenv("FIO_EXE")
	if Exe == "" {
		Exe = "fio"
	}

	dataDir, err := ioutil.TempDir("", "fio-data")
	if err != nil {
		return nil, err
	}

	return &Runner{
		Exe:     Exe,
		DataDir: dataDir,
		Global: Config{
			{
				Name: "global",
				Options: map[string]string{
					"openfiles":         "10",
					"create_fsync":      "0",
					"create_serialize":  "1",
					"file_service_type": "sequential",
					"ioengine":          "libaio",
					"direct":            "1",
					"iodepth":           "32",
					"blocksize":         "1m",
					"refill_buffers":    "",
					"rw":                "write",
					"directory":         dataDir,
				},
			},
		},
	}, nil
}

// Cleanup cleans up the data directory
func (fr *Runner) Cleanup() {
	if fr.DataDir != "" {
		os.RemoveAll(fr.DataDir) //nolint:errcheck
	}
}

// RunConfigs runs fio using the provided Configs
func (fr *Runner) RunConfigs(cfgs ...Config) (stdout, stderr string, err error) {
	var args []string

	// Apply global config before any other configs
	for _, cfg := range append([]Config{fr.Global}, cfgs...) {
		log.Printf("Applying config:\n%s", cfg)

		for _, job := range cfg {
			args = append(args, JobNameFlag, job.Name)
			for flagK, flagV := range job.Options {
				args = append(args, "--"+flagK)

				if flagV != "" {
					args = append(args, flagV)
				}
			}
		}
	}

	return fr.Run(args...)
}

// Run will execute the fio command with the given args
func (fr *Runner) Run(args ...string) (stdout, stderr string, err error) {
	argsStr := strings.Join(args, " ")
	log.Printf("running '%s %v'", fr.Exe, argsStr)
	// nolint:gosec
	c := exec.Command(fr.Exe, args...)

	stderrPipe, err := c.StderrPipe()
	if err != nil {
		return stdout, stderr, err
	}

	var errOut []byte

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		errOut, err = ioutil.ReadAll(stderrPipe)
	}()

	o, err := c.Output()

	wg.Wait()

	log.Printf("finished '%s %v' with err=%v and output:\n%v\n%v", fr.Exe, argsStr, err, string(o), string(errOut))

	return string(o), string(errOut), err
}
