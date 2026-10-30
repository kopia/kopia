//go:build darwin || (linux && amd64)

package engine

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/kopia/kopia/internal/clock"
)

var (
	repoBuildTime   = "unknown"
	repoGitRevision = "unknown"
	repoGitBranch   = "unknown"
	testBuildTime   = "unknown"
	testGitRevision = "unknown"
	testGitBranch   = "unknown"
)

// Stats prints the engine stats, cumulative and from the current run.
func (e *Engine) Stats() string {
	b := &strings.Builder{}

	fmt.Fprintln(b, "==================================")
	fmt.Fprintln(b, "Build Info")
	fmt.Fprintln(b, "==================================")
	fmt.Fprintf(b, "  Repo build time:      %25v\n", repoBuildTime)
	fmt.Fprintf(b, "  Repo git revision:    %25v\n", repoGitRevision)
	fmt.Fprintf(b, "  Repo git branch:      %25v\n", repoGitBranch)
	fmt.Fprintln(b, "")
	fmt.Fprintf(b, "  Engine build time:    %25v\n", testBuildTime)
	fmt.Fprintf(b, "  Engine git revision:  %25v\n", testGitRevision)
	fmt.Fprintf(b, "  Engine git branch:    %25v\n", testGitBranch)
	fmt.Fprintln(b, "")
	fmt.Fprintln(b, "==================================")
	fmt.Fprintln(b, "Engine Action Summary (Cumulative)")
	fmt.Fprintln(b, "==================================")
	fmt.Fprintf(b, "  Engine runtime:   %10vs\n", e.getRuntimeSeconds())
	fmt.Fprintln(b, "")
	fmt.Fprint(b, e.CumulativeStats.Stats())
	fmt.Fprintln(b, "")

	fmt.Fprintln(b, "==================================")
	fmt.Fprintln(b, "Engine Action Summary (This Run)")
	fmt.Fprintln(b, "==================================")
	fmt.Fprint(b, e.RunStats.Stats())
	fmt.Fprintln(b, "")

	return b.String()
}

// Stats tracks statistics during engine runtime.
type Stats struct {
	RunCounter     int64
	ActionCounter  int64
	CreationTime   time.Time
	RunTime        time.Duration
	PerActionStats map[ActionKey]*ActionStats

	DataRestoreCount   int64
	DataPurgeCount     int64
	ErrorRecoveryCount int64
	NoOpCount          int64
}

// Stats returns a string report of the engine's stats.
func (stats *Stats) Stats() string {
	b := &strings.Builder{}

	fmt.Fprintln(b, "=============")
	fmt.Fprintln(b, "Stat summary")
	fmt.Fprintln(b, "=============")
	fmt.Fprintf(b, "  Number of runs:     %10v\n", stats.RunCounter)
	fmt.Fprintf(b, "  Engine lifetime:   %10vs\n", stats.getLifetimeSeconds())
	fmt.Fprintf(b, "  Actions run:        %10v\n", stats.ActionCounter)
	fmt.Fprintf(b, "  Errors recovered:   %10v\n", stats.ErrorRecoveryCount)
	fmt.Fprintf(b, "  Data dir restores:  %10v\n", stats.DataRestoreCount)
	fmt.Fprintf(b, "  Data dir purges:    %10v\n", stats.DataPurgeCount)
	fmt.Fprintf(b, "  NoOp count:         %10v\n", stats.NoOpCount)
	fmt.Fprintln(b, "")
	fmt.Fprintln(b, "=============")
	fmt.Fprintln(b, "Action stats")
	fmt.Fprintln(b, "=============")

	for actionKey, actionStat := range stats.PerActionStats {
		fmt.Fprintf(b, "%s:\n", actionKey)
		fmt.Fprintf(b, "  Count:            %10d\n", actionStat.Count)
		fmt.Fprintf(b, "  Avg Runtime:      %10v\n", actionStat.avgRuntimeString())
		fmt.Fprintf(b, "  Max Runtime:     %10vs\n", durationToSec(actionStat.MaxRuntime))
		fmt.Fprintf(b, "  Min Runtime:     %10vs\n", durationToSec(actionStat.MinRuntime))
		fmt.Fprintf(b, "  Error Count:      %10v\n", actionStat.ErrorCount)
		fmt.Fprintln(b, "")
	}

	return b.String()
}

// ActionStats tracks runtime statistics for an action.
type ActionStats struct {
	Count        int64
	TotalRuntime time.Duration
	MinRuntime   time.Duration
	MaxRuntime   time.Duration
	ErrorCount   int64
}

// AverageRuntime returns the average run time for the action.
func (s *ActionStats) AverageRuntime() time.Duration {
	return time.Duration(int64(s.TotalRuntime) / s.Count)
}

// Record records the current time against the provided start time
// and updates the stats accordingly.
func (s *ActionStats) Record(st time.Time, err error) {
	thisRuntime := clock.Now().Sub(st)
	s.TotalRuntime += thisRuntime

	if thisRuntime > s.MaxRuntime {
		s.MaxRuntime = thisRuntime
	}

	if s.Count == 0 || thisRuntime < s.MinRuntime {
		s.MinRuntime = thisRuntime
	}

	s.Count++

	if err != nil {
		s.ErrorCount++
	}
}

func (stats *Stats) getLifetimeSeconds() int64 {
	return durationToSec(clock.Now().Sub(stats.CreationTime))
}

func durationToSec(dur time.Duration) int64 {
	return int64(dur.Round(time.Second).Seconds())
}

func (s *ActionStats) avgRuntimeString() string {
	if s.Count == 0 {
		return "--"
	}

	return fmt.Sprintf("%vs", durationToSec(s.AverageRuntime()))
}

func (e *Engine) getTimestampS() int64 {
	return e.getRuntimeSeconds()
}

func (e *Engine) getRuntimeSeconds() int64 {
	return durationToSec(e.CumulativeStats.RunTime + clock.Now().Sub(e.RunStats.CreationTime))
}

type statsUpdatetype int

const (
	statsIncrNoOp statsUpdatetype = iota
	statsIncrDataPurge
	statsIncrDataRestore
	statsIncrErrorRecovery
)

func (e *Engine) statsUpdateCounters(updateType statsUpdatetype) {
	e.statsMux.Lock()
	defer e.statsMux.Unlock()

	switch updateType {
	case statsIncrNoOp:
		e.RunStats.NoOpCount++
		e.CumulativeStats.NoOpCount++
	case statsIncrDataPurge:
		e.RunStats.DataPurgeCount++
		e.CumulativeStats.DataPurgeCount++
	case statsIncrDataRestore:
		e.RunStats.DataRestoreCount++
		e.CumulativeStats.DataRestoreCount++
	case statsIncrErrorRecovery:
		e.RunStats.ErrorRecoveryCount++
		e.CumulativeStats.ErrorRecoveryCount++
	}
}

func (e *Engine) statsIncrActionCountAndLog(actionKey ActionKey) {
	e.statsMux.Lock()
	defer e.statsMux.Unlock()

	e.RunStats.ActionCounter++
	e.CumulativeStats.ActionCounter++

	log.Printf(
		"Engine executing ACTION: name=%q actionCount=%v totActCount=%v t=%vs (%vs)",
		actionKey,
		e.RunStats.ActionCounter,
		e.CumulativeStats.ActionCounter,
		e.RunStats.getLifetimeSeconds(),
		e.getRuntimeSeconds(),
	)
}

func (e *Engine) statsUpdatePerAction(actionKey ActionKey, st time.Time, err error) {
	e.statsMux.Lock()
	defer e.statsMux.Unlock()

	if e.RunStats.PerActionStats != nil && e.RunStats.PerActionStats[actionKey] == nil {
		e.RunStats.PerActionStats[actionKey] = new(ActionStats)
	}

	if e.CumulativeStats.PerActionStats != nil && e.CumulativeStats.PerActionStats[actionKey] == nil {
		e.CumulativeStats.PerActionStats[actionKey] = new(ActionStats)
	}

	e.RunStats.PerActionStats[actionKey].Record(st, err)
	e.CumulativeStats.PerActionStats[actionKey].Record(st, err)
}
