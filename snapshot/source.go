package snapshot

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// SourceInfo represents the information about snapshot source.
type SourceInfo struct {
	Host     string `json:"host"`
	UserName string `json:"userName"`
	Path     string `json:"path"`
}

func (ssi SourceInfo) String() string {
	if ssi.Host == "" && ssi.Path == "" && ssi.UserName == "" {
		return "(global)"
	}

	if ssi.Path == "" {
		return fmt.Sprintf("%v@%v", ssi.UserName, ssi.Host)
	}

	return fmt.Sprintf("%v@%v:%v", ssi.UserName, ssi.Host, ssi.Path)
}

// ParseSourceInfo parses a given path in the context of given hostname and username and returns
// SourceInfo. The path may be bare (in which case it's interpreted as local path and canonicalized)
// or may be 'username@host:path' where path, username and host are not processed.
func ParseSourceInfo(path, hostname, username string) (SourceInfo, error) {
	if path == "(global)" {
		return SourceInfo{}, nil
	}

	p1 := strings.Index(path, "@")
	p2 := strings.Index(path, ":")

	if p1 > 0 && p2 > 0 && p1 < p2 && p2 < len(path) {
		return SourceInfo{
			UserName: path[0:p1],
			Host:     path[p1+1 : p2],
			Path:     path[p2+1:],
		}, nil
	}

	if p1 >= 0 && p2 < 0 {
		if p1+1 < len(path) {
			// support @host and user@host without path
			return SourceInfo{
				UserName: path[0:p1],
				Host:     path[p1+1:],
			}, nil
		}

		return SourceInfo{}, errors.Errorf("invalid hostname in %q", path)
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return SourceInfo{}, errors.Errorf("invalid directory: '%s': %s", path, err)
	}

	return SourceInfo{
		Host:     hostname,
		UserName: username,
		Path:     filepath.Clean(absPath),
	}, nil
}
