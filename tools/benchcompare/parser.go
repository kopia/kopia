package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
)

// BenchResult holds the parsed data for a single benchmark comparison.
type BenchResult struct {
	Name     string
	Unit     string
	OldValue float64
	NewValue float64
	DeltaPct float64 // already a percentage, e.g. -17.20
	N        int
	PValue   float64
}

// parseCSV parses benchstat's actual CSV output, which looks like:
//
//	goos: linux          ← config lines (skipped)
//	goarch: amd64
//	pkg: ...
//	,old.txt,,new.txt,,, ← file label row
//	,sec/op,CI,sec/op,CI,vs base,P  ← unit/header row
//	Encode/format=json-48,1.718e-06,1%,1.422e-06,1%,-17.20%,p=0.000 n=10
//	geomean,...          ← skipped
func parseCSV(raw string) ([]BenchResult, error) {
	// Strip file-level config lines (goos:, goarch:, pkg:, cpu:)
	var csvLines []string
	for _, line := range strings.Split(raw, "\n") {
		if isConfigLine(line) {
			continue
		}
		csvLines = append(csvLines, line)
	}

	if len(csvLines) == 0 {
		return nil, fmt.Errorf("no CSV data found after stripping config lines")
	}

	r := csv.NewReader(strings.NewReader(strings.Join(csvLines, "\n")))
	r.FieldsPerRecord = -1 // rows may have different field counts

	allRows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("CSV parse error: %w", err)
	}

	// We need at least: file-label row + unit row + 1 data row
	if len(allRows) < 3 {
		return nil, fmt.Errorf("not enough rows in CSV (got %d)", len(allRows))
	}

	// Row 0: ["", "old.txt", "", "new.txt", "", "", ""]
	// Row 1: ["", "sec/op", "CI", "sec/op", "CI", "vs base", "P"]
	// Row 2+: data rows
	unit := ""
	if len(allRows[1]) > 1 {
		unit = strings.TrimSpace(allRows[1][1])
	}

	var results []BenchResult
	for _, row := range allRows[2:] {
		if len(row) < 6 {
			continue
		}

		name := strings.TrimSpace(row[0])
		// Skip blank rows and the geomean summary row
		if name == "" || strings.EqualFold(name, "geomean") {
			continue
		}

		res := BenchResult{
			Name: name,
			Unit: unit,
		}

		res.OldValue, _ = strconv.ParseFloat(strings.TrimSpace(row[1]), 64)
		res.NewValue, _ = strconv.ParseFloat(strings.TrimSpace(row[3]), 64)

		// Delta is already a percentage string like "-17.20%" or "~" (no change)
		deltaStr := strings.TrimSuffix(strings.TrimSpace(row[5]), "%")
		if deltaStr != "~" && deltaStr != "" {
			res.DeltaPct, _ = strconv.ParseFloat(deltaStr, 64)
		}

		// P-value field looks like "p=0.000 n=10"
		if len(row) > 6 {
			pField := strings.TrimSpace(row[6])
			res.PValue = extractFloat(pField, "p=")
			res.N = extractInt(pField, "n=")
		}

		results = append(results, res)
	}

	return results, nil
}

// isConfigLine returns true for benchstat's file-level config lines.
func isConfigLine(line string) bool {
	for _, prefix := range []string{"goos:", "goarch:", "pkg:", "cpu:"} {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			return true
		}
	}
	return false
}

// extractFloat finds a float after a prefix like "p=" within a string.
func extractFloat(s, prefix string) float64 {
	idx := strings.Index(s, prefix)
	if idx < 0 {
		return 0
	}
	fields := strings.Fields(s[idx+len(prefix):])
	if len(fields) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(fields[0], 64)
	return v
}

// extractInt finds an int after a prefix like "n=" within a string.
func extractInt(s, prefix string) int {
	idx := strings.Index(s, prefix)
	if idx < 0 {
		return 0
	}
	fields := strings.Fields(s[idx+len(prefix):])
	if len(fields) == 0 {
		return 0
	}
	v, _ := strconv.Atoi(fields[0])
	return v
}
