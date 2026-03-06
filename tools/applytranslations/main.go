// Command applytranslations applies translations from a dictionary file
// to the Russian locale file.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

var (
	dictFile = flag.String("dict", "", "Dictionary file with translations (format: English|Russian)")
	localeDir = flag.String("locale", "internal/i18n/locales", "Locale directory")
	verbose   = flag.Bool("verbose", false, "Verbose output")
)

func main() {
	flag.Parse()

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Load dictionary
	dictionary := make(map[string]string)
	if *dictFile != "" {
		data, err := os.ReadFile(*dictFile)
		if err != nil {
			return fmt.Errorf("reading dictionary: %w", err)
		}
		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.SplitN(line, "|", 2)
			if len(parts) == 2 {
				dictionary[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
		fmt.Printf("Loaded %d translations from dictionary\n", len(dictionary))
	}

	// Load English file
	enFile := *localeDir + "/en.yaml"
	enStrings := make(map[string]string)
	enKeys := make([]string, 0)
	
	data, err := os.ReadFile(enFile)
	if err != nil {
		return fmt.Errorf("reading en.yaml: %w", err)
	}
	
	re := regexp.MustCompile(`^"([^"]+)":\s*"(.*)"$`)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if m := re.FindStringSubmatch(line); len(m) > 2 {
			key := m[1]
			enStrings[key] = m[2]
			enKeys = append(enKeys, key)
		}
	}
	sort.Strings(enKeys)
	fmt.Printf("Loaded %d English strings\n", len(enStrings))

	// Load existing Russian translations
	ruFile := *localeDir + "/ru.yaml"
	ruStrings := make(map[string]string)
	
	if data, err := os.ReadFile(ruFile); err == nil {
		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if m := re.FindStringSubmatch(line); len(m) > 2 {
				if !strings.HasPrefix(strings.TrimSpace(line), "#") {
					ruStrings[m[1]] = m[2]
				}
			}
		}
	}
	fmt.Printf("Loaded %d existing Russian translations\n", len(ruStrings))

	// Apply translations
	updated := 0
	for _, key := range enKeys {
		if _, exists := ruStrings[key]; !exists {
			// Try dictionary first
			if *dictFile != "" {
				if trans, ok := dictionary[key]; ok {
					ruStrings[key] = trans
					updated++
					if *verbose {
						fmt.Printf("Translated: %s -> %s\n", key, trans)
					}
					continue
				}
			}
			// Keep as placeholder
			ruStrings[key] = "" // Will be marked as TODO
		}
	}

	// Write Russian file
	f, err := os.Create(ruFile)
	if err != nil {
		return fmt.Errorf("creating ru.yaml: %w", err)
	}
	defer f.Close()

	f.WriteString("# Russian translations for Kopia CLI\n")
	f.WriteString("# See internal/i18n/README.md for translation guidelines\n\n")
	
	for _, key := range enKeys {
		value := ruStrings[key]
		if value == "" {
			fmt.Fprintf(f, "# %q: \"TODO: Translate\"\n", key)
		} else {
			fmt.Fprintf(f, "%q: %q\n", key, value)
		}
	}

	fmt.Printf("Updated %s with %d translations\n", ruFile, len(ruStrings))
	if *dictFile != "" {
		fmt.Printf("Applied %d new translations from dictionary\n", updated)
	}

	return nil
}
