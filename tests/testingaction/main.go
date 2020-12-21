// Command testingaction implements a action that is used in various tests.
package main

import (
	"bufio"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
)

var (
	exitCode              = flag.Int("exit-code", 0, "Exit code")
	sleepDuration         = flag.Duration("sleep", 0, "Sleep duration")
	saveEnvironmentToFile = flag.String("save-env", "", "Save environment to file (key=value).")
	copyFilesSpec         = flag.String("copy-files", "", "Copy files based on spec in the provided file (each line containing 'source => destination')")
	createFile            = flag.String("create-file", "", "Create empty file with a given name")
	writeToStdout         = flag.String("stdout-file", "", "Copy contents of the provided file to stdout.")
	writeToStderr         = flag.String("stderr-file", "", "Copy contents of the provided file to stderr.")
)

func main() {
	flag.Parse()

	if fn := *saveEnvironmentToFile; fn != "" {
		if err := ioutil.WriteFile(fn, []byte(strings.Join(os.Environ(), "\n")), 0600); err != nil {
			log.Fatalf("error writing environment file: %v", err)
		}
	}

	if fn := *writeToStdout; fn != "" {
		if err := writeFileTo(os.Stdout, fn); err != nil {
			log.Fatalf("error writing to stdout: %v", err)
		}
	}

	if fn := *writeToStderr; fn != "" {
		if err := writeFileTo(os.Stderr, fn); err != nil {
			log.Fatalf("error writing to stderr: %v", err)
		}
	}

	if fn := *copyFilesSpec; fn != "" {
		if err := copyFiles(fn); err != nil {
			log.Fatalf("unable to copy files: %v", err)
		}
	}

	if fn := *createFile; fn != "" {
		if _, err := os.Stat(fn); !os.IsNotExist(err) {
			log.Fatalf("unexpected file found: %v", fn)
		}

		if err := ioutil.WriteFile(fn, nil, 0600); err != nil {
			log.Fatalf("unable to create file: %v", err)
		}
	}

	time.Sleep(*sleepDuration)
	os.Exit(*exitCode)
}

func writeFileTo(dst io.Writer, fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return err
	}

	defer f.Close()

	io.Copy(dst, f)

	return nil
}

func copyFiles(specFile string) error {
	f, err := os.Open(specFile)
	if err != nil {
		return errors.Wrap(err, "unable to open spec file")
	}

	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		parts := strings.Split(s.Text(), " => ")
		if len(parts) != 2 {
			continue
		}

		src := os.ExpandEnv(parts[0])
		dst := os.ExpandEnv(parts[1])

		if err := copyFile(src, dst); err != nil {
			return errors.Wrap(err, "copy file error")
		}
	}

	return s.Err()
}

func copyFile(src, dst string) error {
	df, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer df.Close()

	return writeFileTo(df, src)
}
