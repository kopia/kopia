// Package fio wraps calls to the fio tool.
// It assumes the tool is executable by "fio", but
// gives the option to specify another executable
// path by setting environment variable FIO_EXE.
package fio

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

// List of fio flags
const (
	JobNameFlag = "--name"
)

const (
	dockerExe            = "docker"
	fioDataContainerPath = "/fio-data"
)

// Environment variable keys
const (
	// FioExeEnvKey gives the path to the fio executable to use in testing
	FioExeEnvKey = "FIO_EXE"

	// FioDockerImageEnvKey specifies the docker image tag to use. If
	// FioExeEnvKey is set, the local executable will be used instead of
	// docker, even if this variable is also set. The exception is if
	// FioUseDockerEnvKey is not an empty string, which will force
	// use of the fio docker image independent of FioExeEnvKey.
	FioDockerImageEnvKey = "FIO_DOCKER_IMAGE"

	// LocalFioDataPathEnvKey is the local path where fio data will be
	// accessible. If not specified, defaults to the default temp directory (os.TempDir)
	LocalFioDataPathEnvKey = "LOCAL_FIO_DATA_PATH"

	// HostFioDataPathEnvKey specifies the path where fio data will be written,
	// relative to the docker host. If left blank, defaults to local fio data path
	// (works unless running via docker from within a container, e.g. for development)
	HostFioDataPathEnvKey = "HOST_FIO_DATA_PATH"

	// FioUseDockerEnvKey forces the fio runner to use the docker image, even if
	// an executable path is provided.
	FioUseDockerEnvKey = "FIO_USE_DOCKER"
)

// Known error messages
var (
	ErrEnvNotSet = fmt.Errorf("must set either %v or %v", FioExeEnvKey, FioDockerImageEnvKey)
)

// Runner is a helper for running fio commands
type Runner struct {
	Exe             string
	ExecArgs        []string
	LocalDataDir    string
	FioWriteBaseDir string
	Global          Config
}

// NewRunner creates a new fio runner
func NewRunner() (fr *Runner, err error) {
	exeStr := os.Getenv(FioExeEnvKey)
	imgStr := os.Getenv(FioDockerImageEnvKey)
	localFioDataPathStr := os.Getenv(LocalFioDataPathEnvKey)
	hostFioDataPathStr := os.Getenv(HostFioDataPathEnvKey)
	forceDocker := os.Getenv(FioUseDockerEnvKey) != ""

	var exeArgs []string

	var fioWriteBaseDir string

	var Exe string

	dataDir, err := ioutil.TempDir(localFioDataPathStr, "fio-data-")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create temp directory for fio runner")
	}

	switch {
	case exeStr != "" && !forceDocker:
		// Provided a local FIO executable to run
		Exe = exeStr

		fioWriteBaseDir = dataDir

	case imgStr != "":
		// Provided a docker image to run inside
		Exe = dockerExe

		dataDirParent, dataDirName := filepath.Split(dataDir)
		fioWriteBaseDir = filepath.Join(fioDataContainerPath, dataDirName)

		// If the host path wasn't provided, assume it's the same as the local
		// data directory path and we are not running from within a container already
		if hostFioDataPathStr == "" {
			hostFioDataPathStr = dataDirParent
		}

		exeArgs = []string{
			"run",
			"--rm",
			"-v",
			fmt.Sprintf("%s:%s", hostFioDataPathStr, fioDataContainerPath), // /c/Users/usr/fio-data:/fio-data
			imgStr,
		}

	default:
		return nil, ErrEnvNotSet
	}

	fr = &Runner{
		Exe:             Exe,
		ExecArgs:        exeArgs,
		LocalDataDir:    dataDir,
		FioWriteBaseDir: filepath.ToSlash(fioWriteBaseDir),
		Global: Config{
			{
				Name: "global",
				Options: Options{
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
				}.WithDirectory(fioWriteBaseDir),
			},
		},
	}

	err = fr.verifySetupWithTestWrites()
	if err != nil {
		log.Printf("Verify environment setup:\n")
		log.Printf("   Set %s (=%q)to the fio executable\n", FioExeEnvKey, exeStr)
		log.Printf("   - OR -\n")
		log.Printf("   Set %s (=%q) to the fio docker image", FioDockerImageEnvKey, imgStr)
		log.Printf("   Set %s (=%q) to the path where fio data will be used locally", LocalFioDataPathEnvKey, localFioDataPathStr)
		log.Printf("   Set %s (=%q) to the fio data path on the docker host (defaults to %v, if not running in a dev container)", HostFioDataPathEnvKey, os.Getenv(HostFioDataPathEnvKey), LocalFioDataPathEnvKey)

		return nil, errors.Wrap(err, "fio setup could not be validated")
	}

	return fr, nil
}

func (fr *Runner) verifySetupWithTestWrites() error {
	var subDirPath = path.Join("test", "subdir")

	const (
		maxTestFiles = 5
		fileSizeB    = 1 << 20 // 1 MiB
	)

	nrFiles := rand.Intn(maxTestFiles) + 1

	opt := Options{}.WithNumFiles(nrFiles).WithFileSize(fileSizeB)

	err := fr.WriteFiles(subDirPath, opt)
	if err != nil {
		return errors.Wrap(err, "unable to perform writes")
	}

	defer fr.DeleteRelDir("test") //nolint:errcheck

	fl, err := ioutil.ReadDir(filepath.Join(fr.LocalDataDir, subDirPath))
	if err != nil {
		return errors.Wrapf(err, "error reading path %v", subDirPath)
	}

	if got, want := len(fl), nrFiles; got != want {
		return errors.Errorf("did not find the expected number of files %v != %v (expected)", got, want)
	}

	for _, fi := range fl {
		if got, want := fi.Size(), int64(fileSizeB); got != want {
			return errors.Errorf("did not get expected file size from writes %v != %v (expected)", got, want)
		}
	}

	return nil
}

// Cleanup cleans up the data directory
func (fr *Runner) Cleanup() {
	if fr.LocalDataDir != "" {
		os.RemoveAll(fr.LocalDataDir) //nolint:errcheck
	}
}

// RunConfigs runs fio using the provided Configs
func (fr *Runner) RunConfigs(cfgs ...Config) (stdout, stderr string, err error) {
	args := argsFromConfigs(append([]Config{fr.Global}, cfgs...)...)

	return fr.Run(args...)
}

func argsFromConfigs(cfgs ...Config) []string {
	var args []string

	// Apply global config before any other configs
	for _, cfg := range cfgs {
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

	return args
}

// Run will execute the fio command with the given args
func (fr *Runner) Run(args ...string) (stdout, stderr string, err error) {
	args = append(fr.ExecArgs, args...)

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
