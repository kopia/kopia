// Package kopiarunner wraps the execution of the kopia binary.
package kopiarunner

import (
	"bytes"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

const (
	repoPassword = "qWQPJ2hiiLgWRRCr"
)

// Runner is a helper for running kopia commands.
type Runner struct {
	Exe         string
	ConfigDir   string
	fixedArgs   []string
	environment []string
}

// ErrExeVariableNotSet is an exported error.
var ErrExeVariableNotSet = errors.New("KOPIA_EXE variable has not been set")

// NewRunner initializes a new kopia runner and returns its pointer.
func NewRunner(baseDir string) (*Runner, error) {
	exe := os.Getenv("KOPIA_EXE")
	if exe == "" {
		return nil, ErrExeVariableNotSet
	}

	configDir, err := os.MkdirTemp(baseDir, "kopia-config")
	if err != nil {
		return nil, err
	}

	fixedArgs := []string{
		// use per-test config file, to avoid clobbering current user's setup.
		"--config-file", filepath.Join(configDir, ".kopia.config"),
	}

	return &Runner{
		Exe:         exe,
		ConfigDir:   configDir,
		fixedArgs:   fixedArgs,
		environment: []string{"KOPIA_PASSWORD=" + repoPassword},
	}, nil
}

// Cleanup cleans up the directories managed by the kopia Runner.
func (kr *Runner) Cleanup() {
	if kr.ConfigDir != "" {
		os.RemoveAll(kr.ConfigDir) //nolint:errcheck
	}
}

// Run will execute the kopia command with the given args.
func (kr *Runner) Run(args ...string) (stdout, stderr string, err error) {
	argsStr := strings.Join(args, " ")
	log.Printf("running '%s %v'", kr.Exe, argsStr)
	cmdArgs := append(append([]string(nil), kr.fixedArgs...), args...)
	c := exec.Command(kr.Exe, cmdArgs...)
	c.Env = append(os.Environ(), kr.environment...)

	errOut := &bytes.Buffer{}
	c.Stderr = errOut

	o, err := c.Output()
	log.Printf("finished '%s %v' with err=%v and output:\nSTDOUT:\n%v\nSTDERR:\n%v", kr.Exe, argsStr, err, string(o), errOut.String())

	return string(o), errOut.String(), err
}

// RunAsync will execute the kopia command with the given args in background.
func (kr *Runner) RunAsync(args ...string) (*exec.Cmd, error) {
	log.Printf("running async '%s %v'", kr.Exe, strings.Join(args, " "))
	cmdArgs := append(append([]string(nil), kr.fixedArgs...), args...)
	//nolint:gosec //G204
	c := exec.Command(kr.Exe, cmdArgs...)
	c.Env = append(os.Environ(), kr.environment...)
	c.Stderr = &bytes.Buffer{}

	setpdeath(c)

	err := c.Start()
	if err != nil {
		return nil, errors.Wrap(err, "Run async failed for "+kr.Exe)
	}

	return c, nil
}
