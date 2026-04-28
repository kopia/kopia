package storj

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/kopia/kopia/repo/blob/throttling"
)

var (
	testGrant       = "16hYVv3kjmaiEEdx9mvLqgNVAkejuQg6qGyTTwb2u7wDn2KWDb8EeHUrSu7sdDjPqbUZrQDRQXg2B5kKCUiHDvDDKqKYTbpT6MrEYASWnCxJbQhCnTT8yY6zPDXn1txbEditr81oroDEyABUDek3dPWfWmh2XaXMzkCLf91R9QeeerrVPCZGfyuFqciNtFeheCYN7keawj1j5wYnbQBAJC65Qz9JBFGZ3t8N4ovaqYpRqy3dj6YVyen32CUptAQEDrywShd3aB7At1Yuej81gAs58DzL7W5749zB8uzekP4pCyP6X8cpdBd3w99JgeVT239WiE4G2R28xL"
	testPassPhrase  = "KopiaTestStorjPass!"
	testAccessName  = "kopia-storj"
	testBucketName  = "kopia-storj"
	testId          = "my-blob-id-122345"
	optionsTestRepo = Options{
		BucketName:  testBucketName,
		Limits:      throttling.Limits{},
		AccessName:  testAccessName,
		KeyOrGrant:  testGrant,
		Passphrase:  testPassPhrase,
		PointInTime: &time.Time{},
	}

	providerCreds = map[string]*Options{
		"kopia@storj": &optionsTestRepo,
	}
)

func GetUplinkCreds() *Options {
	return providerCreds["kopia@storj"]
}

// readAccessGrant reads an access grant from a file (as exported via satconsole)
// if relpath then the glob is interpreted as relative to the project root
func readAccessGrant(glob string, relpath bool) string {
	var (
		ppfx  string
		err   error
		aglob string
	)
	if relpath {
		ppfx, err = findProjectRoot("go.mod")
		// log.Printf("pwd: %q", ppfx)
		if err != nil {
			log.Fatal(err.Error())
		}
		ppfx = ppfx + "/"
	}
	aglob = ppfx + glob
	files, err := filepath.Glob(aglob)
	if err != nil {
		log.Fatal(err.Error())
	}
	if len(files) == 0 {
		log.Fatalf("no access grant found for glob %q", aglob)
	}
	// Sort files by modification time (newest first)
	sort.Slice(files, func(i, j int) bool {
		infoI, err := os.Stat(files[i])
		if err != nil {
			return false
		}
		infoJ, err := os.Stat(files[j])
		if err != nil {
			return false
		}
		return infoI.ModTime().After(infoJ.ModTime())
	})
	// Read the newest file
	newestFile := files[0]
	content, err := os.ReadFile(newestFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Printf("returning access grant: %q from %s", string(content), newestFile)
	return string(content)
}

// findProjectRoot searches for a known root marker (like `go.mod`).
func findProjectRoot(marker string) (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		candidate := filepath.Join(dir, marker)
		if _, err := os.Stat(candidate); err == nil {
			return dir, nil // Found project root
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break // Reached filesystem root
		}
		dir = parent
	}
	return "", fmt.Errorf("project root not found")
}
