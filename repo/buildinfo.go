package repo

import (
	stdlib "log"
	"runtime/debug"
	"strings"
)

// Kopia's build information.
//
//nolint:gochecknoglobals
var (
	BuildInfo       = ""
	BuildVersion    = ""
	BuildGitHubRepo = ""
)

func init() {
	// fill in from executable's build info when these are unset
	BuildInfo, BuildVersion = getBuildInfoAndVersion(BuildInfo, BuildVersion)
}

func getBuildInfoAndVersion(linkedInfo, linkedVersion string) (info, version string) {
	info, version = linkedInfo, linkedVersion

	if info != "" && version != "" {
		return // use the values specified at link time
	}

	// a value was not set at link time, set it from the executable's build
	// info if available.
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		// logging not yet set up, use stdlib's logging
		stdlib.Println("executable build information is not available")
		return // executable's build info is not available, use values set at link time, if any
	}

	if version == "" {
		version = "v0-unofficial"

		if bi.Main.Version != "" && bi.Main.Version != "(devel)" { // set to '(devel)' during tests in Go 1.24
			version = bi.Main.Version
		}
	}

	if info == "" {
		info = getRevisionString(bi.Settings)
	}

	return
}

func getRevisionString(s []debug.BuildSetting) string {
	var (
		revision, vcsTime string
		modified          bool
	)

	for _, v := range s {
		switch v.Key {
		case "vcs.revision":
			revision = v.Value
		case "vcs.time":
			vcsTime = v.Value
		case "vcs.modified":
			if strings.EqualFold(v.Value, "true") {
				modified = true
			}
		}
	}

	if revision == "" {
		revision = "(unknown_revision)"
	}

	var modStr string

	if modified {
		modStr = "+dirty"
	}

	return vcsTime + "-" + revision + modStr
}
