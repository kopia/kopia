package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var (
	psrecordRegex = regexp.MustCompile(`psrecord-(.*)-(initial|second)-(.*).log`)
	reposizeRegex = regexp.MustCompile(`repo-size-(.*?)-(.*).log`)
)

type processStats struct {
	duration float64
	avgCPU   float64
	avgRAM   float64
	maxCPU   float64
	maxRAM   float64
}

var (
	processStatsByScenarioAndVersion = map[string]map[string]processStats{}
	repoSizeByScenarioArndVersion    = map[string]map[string]int64{}
)

// getProcessStats parses psrecord log file and computes statistics.
func getProcessStats(fname string) (processStats, error) {
	f, err := os.Open(fname) //nolint:gosec
	if err != nil {
		return processStats{}, err
	}
	defer f.Close() //nolint:errcheck

	s := bufio.NewScanner(f)

	s.Scan() // skip first line

	var (
		ps            processStats
		totalCPUUsage float64
		totalRAMUsage float64
		maxCPUUsage   float64
		maxRAMUsage   float64
		sampleCount   float64
	)

	for s.Scan() {
		fields := strings.Fields(s.Text())

		ts, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return ps, errors.Wrap(err, "error parsing time")
		}

		cpuUsage, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return ps, errors.Wrap(err, "error parsing cpu")
		}

		totalCPUUsage += cpuUsage

		if cpuUsage > maxCPUUsage {
			maxCPUUsage = cpuUsage
		}

		ramUsage, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			return ps, errors.Wrap(err, "error parsing ram")
		}

		totalRAMUsage += ramUsage

		if ramUsage > maxRAMUsage {
			maxRAMUsage = ramUsage
		}

		sampleCount++

		ps.duration = ts
	}

	if sampleCount > 0 {
		ps.maxCPU = maxCPUUsage
		ps.maxRAM = maxRAMUsage
		ps.avgCPU = totalCPUUsage / sampleCount
		ps.avgRAM = totalRAMUsage / sampleCount
	}

	return ps, nil
}

func parseRepoSize(fname string) (int64, error) {
	f, err := os.Open(fname) //nolint:gosec
	if err != nil {
		return 0, err
	}
	defer f.Close() //nolint:errcheck

	s := bufio.NewScanner(f)
	s.Scan()

	fields := strings.Fields(s.Text())
	if len(fields) != 2 {
		return 0, errors.New("invalid repo size format")
	}

	return strconv.ParseInt(fields[0], 10, 64)
}

func main() {
	files, err := os.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		if m := psrecordRegex.FindStringSubmatch(f.Name()); m != nil {
			version := m[1]
			phase := m[2]
			scenario := m[3]

			ps, err := getProcessStats(f.Name())
			if err != nil {
				log.Fatalf("err: %v", err)
			}

			if phase == "initial" {
				ss := processStatsByScenarioAndVersion[scenario]
				if ss == nil {
					ss = map[string]processStats{}
					processStatsByScenarioAndVersion[scenario] = ss
				}

				ss[version] = ps
			}
		}

		if m := reposizeRegex.FindStringSubmatch(f.Name()); m != nil {
			version := m[1]
			scenario := m[2]

			rs, err := parseRepoSize(f.Name())
			if err != nil {
				log.Fatalf("unable to parse repo size: %v", err)
			}

			ss := repoSizeByScenarioArndVersion[scenario]
			if ss == nil {
				ss = map[string]int64{}
				repoSizeByScenarioArndVersion[scenario] = ss
			}

			ss[version] = rs
		}
	}

	for scenario, results := range processStatsByScenarioAndVersion {
		for ver, ps := range results {
			rs := repoSizeByScenarioArndVersion[scenario][ver]
			fmt.Printf("%v,%v,%v,%v,%v,%v,%v,%v\n", scenario, ver, ps.duration, ps.avgCPU, ps.maxCPU, ps.avgRAM, ps.maxRAM, rs)
		}
	}
}
