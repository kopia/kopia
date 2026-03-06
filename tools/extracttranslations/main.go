// Command extractTranslations extracts translatable strings from Go source files
// and generates translation templates.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var (
	sourceDir   = flag.String("source", "cli", "Source directory to scan")
	outputDir   = flag.String("output", "internal/i18n/locales", "Output directory for locale files")
	verbose     = flag.Bool("verbose", false, "Verbose output")
	dryRun      = flag.Bool("dry-run", false, "Dry run - show what would be done")
)

// Patterns to extract translatable strings
var patterns = []struct {
	regex *regexp.Regexp
	name  string
}{
	{regexp.MustCompile(`\.Command\("[^"]+",\s*"([^"]+)"\)`), "Command"},
	{regexp.MustCompile(`\.Flag\("[^"]+",\s*"([^"]+)"\)`), "Flag"},
	{regexp.MustCompile(`\.Alias\("([^"]+)"\)`), "Alias"},
}

func main() {
	flag.Parse()

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	translationStrings := make(map[string]string)

	// Walk through source directory
	err := filepath.Walk(*sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		// Skip test files
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}

		if *verbose {
			fmt.Fprintf(os.Stderr, "Processing: %s\n", path)
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		lineNum := 0
		for scanner.Scan() {
			lineNum++
			line := scanner.Text()

			for _, p := range patterns {
				matches := p.regex.FindAllStringSubmatch(line, -1)
				for _, m := range matches {
					if len(m) > 1 {
						str := m[1]
						// Skip empty strings and very short strings
						if len(str) > 2 {
							key := str
							if _, exists := translationStrings[key]; !exists {
								translationStrings[key] = str // Default to English
								if *verbose {
									fmt.Printf("Found [%s]: %s\n", p.name, str)
								}
							}
						}
					}
				}
			}
		}

		return scanner.Err()
	})

	if err != nil {
		return fmt.Errorf("walking source: %w", err)
	}

	// Sort keys
	keys := make([]string, 0, len(translationStrings))
	for k := range translationStrings {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Read existing English translations
	enFile := filepath.Join(*outputDir, "en.yaml")
	existingEn := make(map[string]string)
	if data, err := os.ReadFile(enFile); err == nil {
		re := regexp.MustCompile(`^"([^"]+)":\s*"(.*)"$`)
		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if m := re.FindStringSubmatch(line); len(m) > 2 {
				existingEn[m[1]] = m[2]
			}
		}
	}

	// Find new strings
	newStrings := make([]string, 0)
	for _, k := range keys {
		if _, exists := existingEn[k]; !exists {
			newStrings = append(newStrings, k)
		}
	}

	fmt.Printf("Total strings found: %d\n", len(keys))
	fmt.Printf("New strings to add: %d\n", len(newStrings))

	if !*dryRun {
		// Append new strings to English file
		if len(newStrings) > 0 {
			f, err := os.OpenFile(enFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("opening en.yaml: %w", err)
			}
			defer f.Close()

			f.WriteString("\n# Auto-extracted strings\n")
			for _, s := range newStrings {
				escaped := strings.ReplaceAll(s, `"`, `\"`)
				fmt.Fprintf(f, "%q: %q\n", s, escaped)
			}
			fmt.Printf("Added %d new strings to %s\n", len(newStrings), enFile)
		}

		// Update Russian file with placeholders for missing translations
		ruFile := filepath.Join(*outputDir, "ru.yaml")
		existingRu := make(map[string]string)
		if data, err := os.ReadFile(ruFile); err == nil {
			re := regexp.MustCompile(`^#?\s*"([^"]+)":\s*"(.*)"$`)
			scanner := bufio.NewScanner(strings.NewReader(string(data)))
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if m := re.FindStringSubmatch(line); len(m) > 2 {
					if !strings.HasPrefix(line, "#") {
						existingRu[m[1]] = m[2]
					}
				}
			}
		}

		missingRu := make([]string, 0)
		for _, k := range keys {
			if _, exists := existingRu[k]; !exists {
				missingRu = append(missingRu, k)
			}
		}

		if len(missingRu) > 0 {
			f, err := os.OpenFile(ruFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("opening ru.yaml: %w", err)
			}
			defer f.Close()

			f.WriteString("\n# Auto-extracted strings - TODO: Translate\n")
			for _, s := range missingRu {
				escaped := strings.ReplaceAll(s, `"`, `\"`)
				fmt.Fprintf(f, "# %q: \"ПЕРЕВОД\"\n", s, escaped)
			}
			fmt.Printf("Added %d placeholders to %s\n", len(missingRu), ruFile)
		}
	}

	return nil
}
