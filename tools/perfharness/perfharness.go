// Command perfharness runs the provided binary, captures stats and sends them to a server.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/process"
	"golang.org/x/sync/errgroup"
)

var (
	storageDirectory = flag.String("storage-dir", "", "Storage directory to summarize")
	scenario         = flag.String("scenario", "", "Name of the scenario")
	collector        = flag.String("collector", "", "HTTP endpoint to which to post the stats")
)

// stats is a JSON structure that is sent to the collector.
type stats struct {
	Scenario      string        `json:"scenario"`
	MaxRSS        uint64        `json:"maxRSS"`
	MaxCPUPercent float64       `json:"maxCPUPercent"`
	AvgCPUPercent float64       `json:"avgCPUPercent"`
	Duration      time.Duration `json:"duration"`

	DirCount      int   `json:"numDirs"`
	FileCount     int   `json:"numFiles"`
	TotalFileSize int64 `json:"totalFileSize"`

	cpuPercentSamples int
	totalCPUPercent   float64
}

func (s *stats) collectOnce(pi *process.Process) error {
	mi, err := pi.MemoryInfo()
	if err != nil {
		return errors.Wrap(err, "can't get memory info")
	}

	if mi.RSS > s.MaxRSS {
		s.MaxRSS = mi.RSS
	}

	percent, err := pi.CPUPercent()
	if err != nil {
		return errors.Wrap(err, "can't get CPU info")
	}

	s.totalCPUPercent += percent
	s.cpuPercentSamples++

	if percent > s.MaxCPUPercent {
		s.MaxCPUPercent = percent
	}

	return nil
}

func (s *stats) collectUntilContextCancelled(ctx context.Context, pi *process.Process) error {
	if err := s.collectOnce(pi); err != nil {
		return errors.Wrap(err, "error collecting")
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-time.After(1 * time.Second):
			if err := s.collectOnce(pi); err != nil {
				return errors.Wrap(err, "error collecting")
			}
		}
	}
}

func (s *stats) summarizeDirectory(dirname string) error {
	entries, err := ioutil.ReadDir(dirname)
	if err != nil {
		return errors.Wrap(err, "error reading directory")
	}

	for _, e := range entries {
		if e.IsDir() {
			s.DirCount++

			if err := s.summarizeDirectory(filepath.Join(dirname, e.Name())); err != nil {
				return err
			}

			continue
		}

		s.FileCount++
		s.TotalFileSize += e.Size()
	}

	return nil
}

func (s *stats) runProcess(args []string) error {
	c := exec.Command(args[0], args[1:]...) //nolint:gosec
	c.Stderr = os.Stderr
	c.Stdout = os.Stdout

	t0 := time.Now()

	if err := c.Start(); err != nil {
		return errors.Wrap(err, "unable to start process")
	}

	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	var eg errgroup.Group

	eg.Go(func() error {
		p := c.Process
		if p == nil {
			return errors.Errorf("process was nil")
		}

		pi, err := process.NewProcess(int32(p.Pid))
		if err != nil {
			return errors.Errorf("unable to attach to process")
		}

		return s.collectUntilContextCancelled(ctx, pi)
	})

	if err := c.Wait(); err != nil {
		return errors.Wrap(err, "wait error")
	}

	if s.cpuPercentSamples > 0 {
		s.AvgCPUPercent = s.totalCPUPercent / float64(s.cpuPercentSamples)
	}

	s.Duration = time.Since(t0)

	cancelContext()

	return eg.Wait()
}

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 || *scenario == "" {
		log.Fatalf("usage: procmon --scenario=<name> [--storage-dir=<dir>] [--collector=<endpoint>] <command> [args]")
	}

	st := &stats{
		Scenario: *scenario,
	}

	if err := st.runProcess(flag.Args()); err != nil {
		log.Fatalf("error running process: %v", err)
	}

	if dir := *storageDirectory; dir != "" {
		if err := st.summarizeDirectory(dir); err != nil {
			log.Fatalf("error summarizing directory: %v", err)
		}
	}

	var buf bytes.Buffer

	e := json.NewEncoder(io.MultiWriter(&buf, os.Stdout))
	e.SetIndent("", "  ")
	e.Encode(st) //nolint:errcheck

	if *collector != "" {
		resp, err := http.Post(*collector, "application/json", &buf)
		if err != nil {
			log.Fatalf("error sending results to collector: %v", err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			log.Printf("collector error: %v", err)
		}
	}
}
