package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	// Define optional flags.
	oldLabel := flag.String("old", "", "Label for the baseline file (default: filename)")
	newLabel := flag.String("new", "", "Label for the candidate file (default: filename)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: benchcompare [flags] old.txt new.txt\n\n")
		fmt.Fprintf(os.Stderr, "Compares two Go benchmark output files using benchstat and\n")
		fmt.Fprintf(os.Stderr, "produces a classified report with plain-English interpretation.\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nRequires: benchstat (go install golang.org/x/perf/cmd/benchstat@latest)\n")
	}
	flag.Parse()

	args := flag.Args()
	if len(args) != 2 {
		flag.Usage()
		os.Exit(1)
	}

	oldFile, newFile := args[0], args[1]

	// Default labels to the filenames if not overridden.
	if *oldLabel == "" {
		*oldLabel = oldFile
	}
	if *newLabel == "" {
		*newLabel = newFile
	}

	// Verify both files exist before calling benchstat.
	for _, f := range []string{oldFile, newFile} {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "error: file not found: %s\n", f)
			os.Exit(1)
		}
	}

	// Run benchstat and capture its CSV output.
	csv, err := runBenchstat(oldFile, newFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if csv == "" {
		fmt.Println("benchstat produced no output. Are there matching benchmark names in both files?")
		os.Exit(0)
	}

	// Parse the CSV.
	results, err := parseCSV(csv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing benchstat output: %v\n", err)
		fmt.Fprintf(os.Stderr, "raw output was:\n%s\n", csv)
		os.Exit(1)
	}

	// Print the report.
	PrintReport(results, *oldLabel, *newLabel)
}
