package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

const (
	pThreshold    = 0.05 // standard significance threshold
	trivialDelta  = 2.0  // % change too small to care about
	moderateDelta = 10.0 // % change worth investigating
	severeDelta   = 25.0 // % change that should block a PR
)

// Verdict classifies a single benchmark result.
type Verdict string

const (
	VerdictNoChange    Verdict = "NO CHANGE"
	VerdictImprovement Verdict = "IMPROVEMENT"
	VerdictTrivial     Verdict = "TRIVIAL"
	VerdictModerate    Verdict = "REGRESSION (moderate)"
	VerdictSevere      Verdict = "REGRESSION (severe)"
)

type classified struct {
	BenchResult
	verdict Verdict
}

// classify decides the verdict for a single result.
// For time-based units (sec/op, ns/op), positive delta = slower = regression.
// For throughput units (ops/s, MB/s), positive delta = faster = improvement.
func classify(r BenchResult) Verdict {
	// Not statistically significant — don't report as a change.
	if r.PValue >= pThreshold {
		return VerdictNoChange
	}

	// Determine if this unit is "lower is better" or "higher is better".
	lowerIsBetter := isLowerBetter(r.Unit)

	// From the perspective of "is this a regression":
	// - Lower-is-better units: positive delta = worse
	// - Higher-is-better units: negative delta = worse
	regressionDelta := r.DeltaPct
	if !lowerIsBetter {
		regressionDelta = -r.DeltaPct
	}

	absDelta := math.Abs(r.DeltaPct)

	if regressionDelta < 0 {
		// It's an improvement.
		if absDelta < trivialDelta {
			return VerdictTrivial
		}
		return VerdictImprovement
	}

	// It's a regression.
	if absDelta < trivialDelta {
		return VerdictTrivial
	}
	if absDelta < severeDelta {
		return VerdictModerate
	}
	return VerdictSevere
}

// isLowerBetter returns true for units where smaller values are faster/better.
func isLowerBetter(unit string) bool {
	lower := strings.ToLower(unit)
	// Throughput and rate units: higher is better.
	for _, good := range []string{"ops/s", "mb/s", "gb/s", "b/s"} {
		if strings.Contains(lower, good) {
			return false
		}
	}
	// Time and allocation units: lower is better.
	return true
}

// PrintReport prints the full comparison report to stdout.
func PrintReport(results []BenchResult, oldLabel, newLabel string) {
	if len(results) == 0 {
		fmt.Println("No benchmark results to compare.")
		return
	}

	// Classify everything first.
	var rows []classified
	for _, r := range results {
		rows = append(rows, classified{r, classify(r)})
	}

	// Sort: severe regressions first, then moderate, then improvements, then no change.
	order := map[Verdict]int{
		VerdictSevere:      0,
		VerdictModerate:    1,
		VerdictImprovement: 2,
		VerdictTrivial:     3,
		VerdictNoChange:    4,
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return order[rows[i].verdict] < order[rows[j].verdict]
	})

	// Counters for the summary.
	counts := make(map[Verdict]int)
	for _, row := range rows {
		counts[row.verdict]++
	}

	// ── Header ────────────────────────────────────────────────────────────────
	fmt.Printf("\n╔══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║           BENCHMARK COMPARISON REPORT                    ║\n")
	fmt.Printf("╚══════════════════════════════════════════════════════════╝\n")
	fmt.Printf("  baseline : %s\n", oldLabel)
	fmt.Printf("  candidate: %s\n\n", newLabel)

	// ── Per-benchmark table ───────────────────────────────────────────────────
	fmt.Printf("%-55s %-8s %10s %10s %10s %8s  %s\n",
		"BENCHMARK", "UNIT", "OLD", "NEW", "DELTA", "P-VALUE", "VERDICT")
	fmt.Println(strings.Repeat("─", 120))

	for _, row := range rows {
		if row.verdict == VerdictNoChange {
			continue // print these separately at the bottom to reduce noise
		}
		fmt.Printf("%-55s %-8s %10s %10s %+9.2f%% %8.4f  %s\n",
			truncate(row.Name, 55),
			row.Unit,
			formatSI(row.OldValue, row.Unit),
			formatSI(row.NewValue, row.Unit),
			row.DeltaPct,
			row.PValue,
			row.verdict,
		)
	}

	// Print no-change rows dimmed.
	if counts[VerdictNoChange] > 0 {
		fmt.Println(strings.Repeat("─", 120))
		for _, row := range rows {
			if row.verdict != VerdictNoChange {
				continue
			}
			fmt.Printf("%-55s %-8s %10s %10s %10s %8.4f  %s\n",
				truncate(row.Name, 55),
				row.Unit,
				formatSI(row.OldValue, row.Unit),
				formatSI(row.NewValue, row.Unit),
				"~",
				row.PValue,
				row.verdict,
			)
		}
	}

	// ── Summary ───────────────────────────────────────────────────────────────
	fmt.Printf("\n%s\n", strings.Repeat("═", 120))
	fmt.Println("SUMMARY")
	fmt.Printf("  Total benchmarks compared : %d\n", len(rows))
	fmt.Printf("  Severe regressions        : %d\n", counts[VerdictSevere])
	fmt.Printf("  Moderate regressions      : %d\n", counts[VerdictModerate])
	fmt.Printf("  Improvements              : %d\n", counts[VerdictImprovement])
	fmt.Printf("  Trivial / no change       : %d\n", counts[VerdictTrivial]+counts[VerdictNoChange])

	// ── Interpretation ────────────────────────────────────────────────────────
	fmt.Printf("\n%s\n", strings.Repeat("─", 120))
	fmt.Println("INTERPRETATION")
	interpret(rows, counts)
}

// interpret prints a plain-English paragraph summarizing findings.
func interpret(rows []classified, counts map[Verdict]int) {
	total := len(rows)
	if total == 0 {
		return
	}

	severe := counts[VerdictSevere]
	moderate := counts[VerdictModerate]
	improved := counts[VerdictImprovement]

	if severe == 0 && moderate == 0 && improved == 0 {
		fmt.Println("  ✓ No statistically significant changes detected. The candidate is")
		fmt.Println("    performance-equivalent to the baseline at the p=0.05 threshold.")
		fmt.Println("    If you expected a change, try running with -count=20 to reduce noise.")
		return
	}

	if severe > 0 {
		fmt.Printf("  ✗ %d benchmark(s) show a SEVERE regression (>%.0f%% slower).\n",
			severe, severeDelta)
		fmt.Println("    These should be investigated before merging. Look for:")
		fmt.Println("    - New allocations on the hot path (check B/op and allocs/op)")
		fmt.Println("    - Lock contention (use go test -race or pprof mutex profile)")
		fmt.Println("    - Algorithmic complexity change (O(n) → O(n²))")
	}

	if moderate > 0 {
		fmt.Printf("  ⚠  %d benchmark(s) show a MODERATE regression (%.0f–%.0f%% slower).\n",
			moderate, trivialDelta, severeDelta)
		fmt.Println("    Worth profiling, but may be acceptable depending on the change.")
	}

	if improved > 0 {
		fmt.Printf("  ✓  %d benchmark(s) improved. Verify the improvement is real:\n", improved)
		fmt.Println("    - Run with -count=20 to confirm (10 samples can be noisy)")
		fmt.Println("    - Check that the benchmark still exercises the same code path")
	}

	// Specific call-outs for the worst regressions.
	if severe > 0 {
		fmt.Println("\n  Worst regressions:")
		printed := 0
		for _, row := range rows {
			if row.verdict == VerdictSevere && printed < 3 {
				fmt.Printf("    • %s  %+.1f%% (p=%.4f)\n",
					row.Name, row.DeltaPct, row.PValue)
				printed++
			}
		}
	}
}

// ── Helpers ────────────────────────────────────────────────────────────────

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// formatSI converts a raw float64 in SI base units to a human-readable string.
// benchstat CSV gives times in seconds; we convert to ns/µs/ms/s as appropriate.
func formatSI(v float64, unit string) string {
	lower := strings.ToLower(unit)
	if strings.Contains(lower, "sec") || strings.Contains(lower, "ns/op") {
		// Value is in seconds.
		switch {
		case v < 1e-6:
			return fmt.Sprintf("%.1fns", v*1e9)
		case v < 1e-3:
			return fmt.Sprintf("%.1fµs", v*1e6)
		case v < 1:
			return fmt.Sprintf("%.1fms", v*1e3)
		default:
			return fmt.Sprintf("%.2fs", v)
		}
	}
	if strings.Contains(lower, "b/op") {
		return fmt.Sprintf("%.0fB", v)
	}
	return fmt.Sprintf("%.4g", v)
}
