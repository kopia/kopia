// Package gettool combines and replaces curl, tar and gunzip, sha256sum and a bunch of Makefile scripts
// to quickly download, verify and install OS-specific version of tools (typically from GitHub)
// in a platform-agnostic manner without external tooling.
package main

import (
	"bufio"
	_ "embed"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/tools/gettool/autodownload"
)

// ToolInfo encapsulates all information required to download a tool.
type ToolInfo struct {
	urlTemplate         string
	osMap               map[string]string
	archMap             map[string]string
	stripPathComponents int
	unsupportedArch     map[string]bool
	unsupportedOSArch   map[string]bool
	macosUniversalArch  string
}

func (ti ToolInfo) actualURL(version, goos, goarch string) string {
	if ti.unsupportedArch[goarch] {
		return ""
	}

	if ti.unsupportedOSArch[goos+"/"+goarch] {
		return ""
	}

	u := ti.urlTemplate
	u = strings.ReplaceAll(u, "VERSION", version)

	if goos == "darwin" && ti.macosUniversalArch != "" {
		goarch = ti.macosUniversalArch
	}

	u = strings.ReplaceAll(u, "GOARCH", replacementFromMap(goarch, ti.archMap))
	u = strings.ReplaceAll(u, "GOOS", replacementFromMap(goos, ti.osMap))
	u = strings.ReplaceAll(u, "EXT", replacementFromMap(goos, map[string]string{
		"windows": "zip",
		"linux":   "tar.gz",
		"darwin":  "tar.gz",
	}))

	return u
}

//nolint:gochecknoglobals
var tools = map[string]ToolInfo{
	"linter": {
		urlTemplate: "https://github.com/golangci/golangci-lint/releases/download/vVERSION/golangci-lint-VERSION-GOOS-GOARCH.EXT",
		archMap: map[string]string{
			"arm": "armv6",
		},
		stripPathComponents: 1,
	},
	"hugo": {
		urlTemplate: "https://github.com/gohugoio/hugo/releases/download/vVERSION/hugo_extended_VERSION_GOOS-GOARCH.EXT",
		unsupportedArch: map[string]bool{
			"arm": true,
		},
		unsupportedOSArch: map[string]bool{
			"linux/arm64": true,
		},
		macosUniversalArch: "universal",
	},
	"gotestsum": {
		urlTemplate: "https://github.com/gotestyourself/gotestsum/releases/download/vVERSION/gotestsum_VERSION_GOOS_GOARCH.tar.gz",
		archMap: map[string]string{
			"arm": "armv6",
		},
	},
	"kopia": {
		urlTemplate: "https://github.com/kopia/kopia/releases/download/vVERSION/kopia-VERSION-GOOS-GOARCH.EXT",
		archMap: map[string]string{
			"amd64": "x64",
		},
		osMap: map[string]string{
			"darwin": "macOS",
		},
		stripPathComponents: 1,
	},
	"rclone": {
		urlTemplate:         "https://github.com/rclone/rclone/releases/download/vVERSION/rclone-vVERSION-GOOS-GOARCH.zip",
		osMap:               map[string]string{"darwin": "osx"},
		stripPathComponents: 1,
	},
	"goreleaser": {
		urlTemplate: "https://github.com/goreleaser/goreleaser/releases/download/VERSION/goreleaser_GOOS_GOARCH.EXT",
		archMap: map[string]string{
			"amd64": "x86_64",
			"arm":   "armv6",
		},
		osMap: map[string]string{
			"darwin":  "Darwin",
			"linux":   "Linux",
			"windows": "Windows",
		},
	},
	"gitchglog": {
		urlTemplate: "https://github.com/git-chglog/git-chglog/releases/download/vVERSION/git-chglog_VERSION_GOOS_GOARCH.EXT",
		archMap: map[string]string{
			"arm": "armv6",
		},
	},
	"node": {
		urlTemplate:         "https://nodejs.org/dist/vVERSION/node-vVERSION-GOOS-GOARCH.EXT",
		osMap:               map[string]string{"windows": "win"},
		archMap:             map[string]string{"arm": "armv7l", "amd64": "x64"},
		stripPathComponents: 1,
	},
}

//nolint:gochecknoglobals
var (
	tool      = flag.String("tool", "", "Name of the tool:version")
	outputDir = flag.String("output-dir", "", "Output directory")
	goos      = flag.String("goos", runtime.GOOS, "Override GOOS")
	goarch    = flag.String("goarch", runtime.GOARCH, "Override GOARCH")

	testAll             = flag.Bool("test-all", false, "Unpacks the package for all GOOS/ARCH combinations")
	regenerateChecksums = flag.String("regenerate-checksums", "", "Regenerate checksums")
)

//nolint:gochecknoglobals
var buildArchitectures = []struct {
	goos   string
	goarch string
}{
	{"linux", "amd64"},
	{"linux", "arm64"},
	{"linux", "arm"},
	{"darwin", "amd64"},
	{"darwin", "arm64"},
	{"windows", "amd64"},
}

func replacementFromMap(defaultValue string, m map[string]string) string {
	if v, ok := m[defaultValue]; ok {
		return v
	}

	return defaultValue
}

//go:embed checksums.txt
var checksumsFileContents string

func parseEmbeddedChecksums() map[string]string {
	m := map[string]string{}

	s := bufio.NewScanner(strings.NewReader(checksumsFileContents))
	for s.Scan() {
		p := strings.Split(s.Text(), ": ")

		m[p[0]] = p[1]
	}

	return m
}

func main() {
	flag.Parse()

	if *outputDir == "" {
		log.Fatalf("--output-dir must be set")
	}

	checksums := parseEmbeddedChecksums()
	downloadedChecksums := map[string]string{}

	var errorCount int

	for _, toolNameVersion := range strings.Split(*tool, ",") {
		parts := strings.Split(toolNameVersion, ":")

		//nolint:mnd
		if len(parts) != 2 {
			log.Fatalf("invalid tool spec, must be tool:version[,tool:version]")
		}

		toolName := parts[0]
		toolVersion := parts[1]

		if err := downloadTool(toolName, toolVersion, checksums, downloadedChecksums, &errorCount); err != nil {
			log.Fatalf("unable to download %v version %v: %v", toolName, toolVersion, err)
		}
	}

	// all good
	if errorCount == 0 && *regenerateChecksums == "" {
		return
	}

	// on failure print current checksums, so they can be copy/pasted as the new baseline
	var lines []string

	for k, v := range downloadedChecksums {
		lines = append(lines, fmt.Sprintf("%v: %v", k, v))
	}

	sort.Strings(lines)

	for _, l := range lines {
		fmt.Println(l)
	}

	if *regenerateChecksums != "" {
		if err := writeLinesToFile(lines); err != nil {
			log.Fatal(err)
		}

		return
	}

	log.Fatalf("Error(s) encountered, see log messages above.")
}

func writeLinesToFile(lines []string) error {
	f, err := os.Create(*regenerateChecksums)
	if err != nil {
		return errors.Wrap(err, "writeLinesToFile")
	}

	defer f.Close() //nolint:errcheck

	for _, l := range lines {
		fmt.Fprintln(f, l) //nolint:errcheck
	}

	return nil
}

func downloadTool(toolName, toolVersion string, oldChecksums, downloadedChecksums map[string]string, errorCount *int) error {
	t, ok := tools[toolName]
	if !ok {
		return errors.Errorf("unsupported tool: %q", toolName)
	}

	if *testAll {
		for _, ba := range buildArchitectures {
			u := t.actualURL(toolVersion, ba.goos, ba.goarch)
			if u == "" {
				continue
			}

			if err := autodownload.Download(u, filepath.Join(*outputDir, ba.goos, ba.goarch), oldChecksums, t.stripPathComponents); err != nil {
				log.Printf("ERROR %v: %v", u, err)

				*errorCount++
			}
		}

		return nil
	}

	if *regenerateChecksums != "" {
		for _, ba := range buildArchitectures {
			u := t.actualURL(toolVersion, ba.goos, ba.goarch)
			if u == "" {
				continue
			}

			if oldChecksums[u] != "" {
				downloadedChecksums[u] = oldChecksums[u]
				continue
			}

			log.Printf("downloading %v...", u)

			if err := autodownload.Download(u, filepath.Join(*outputDir, ba.goos, ba.goarch), downloadedChecksums, t.stripPathComponents); err != nil {
				log.Printf("ERROR %v: %v", u, err)

				*errorCount++
			}
		}

		return nil
	}

	u := t.actualURL(toolVersion, *goos, *goarch)
	if u == "" {
		log.Fatalf("Tool '%v' is not supported on %v/%v", toolName, *goos, *goarch)
	}

	fmt.Printf("Downloading %v version %v from %v...\n", toolName, toolVersion, u)

	if err := autodownload.Download(u, *outputDir, oldChecksums, t.stripPathComponents); err != nil {
		return errors.Wrap(err, "unable to download")
	}

	return nil
}
