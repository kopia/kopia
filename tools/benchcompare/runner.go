package main

import (
	"fmt"
	"os/exec"
	"strings"
)

// runBenchstat invokes `benchstat -format=csv oldFile newFile` and returns
// the raw CSV output. It returns an error if benchstat isn't installed or
// if the command fails for any reason.
func runBenchstat(oldFile, newFile string) (string, error) {
	cmd := exec.Command("benchstat", "-format=csv", oldFile, newFile)

	// CombinedOutput runs the command and captures both stdout and stderr.
	// We use Output() instead so we can separate them: benchstat writes
	// warnings to stderr and results to stdout.
	stdout, err := cmd.Output()
	if err != nil {
		// exec.ExitError means the process ran but exited non-zero.
		// Other errors mean benchstat wasn't found at all.
		if exitErr, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("benchstat failed: %s", string(exitErr.Stderr))
		}
		return "", fmt.Errorf("could not run benchstat (is it installed?): %w", err)
	}

	return strings.TrimSpace(string(stdout)), nil
}
