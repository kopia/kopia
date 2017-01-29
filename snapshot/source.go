package snapshot

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

var zeroByte = []byte{0}

// SourceInfo represents the information about snapshot source.
type SourceInfo struct {
	Host     string `json:"host"`
	UserName string `json:"userName"`
	Path     string `json:"path"`
}

func (ssi SourceInfo) String() string {
	return fmt.Sprintf("%v@%v:%v", ssi.UserName, ssi.Host, ssi.Path)
}

// ParseSourceInfo parses a given path in the context of given hostname and username and returns
// SourceInfo. The path may be bare (in which case it's interpreted as local path and canonicalized)
// or may be 'username@host:path' where path, username and host are not processed.
func ParseSourceInfo(path string, hostname string, username string) (SourceInfo, error) {
	p1 := strings.Index(path, "@")
	p2 := strings.Index(path, ":")

	if p1 > 0 && p2 > 0 && p1 < p2 && p2 < len(path) {
		return SourceInfo{
			UserName: path[0:p1],
			Host:     path[p1+1 : p2],
			Path:     path[p2+1:],
		}, nil
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return SourceInfo{}, fmt.Errorf("invalid directory: '%s': %s", path, err)
	}

	return SourceInfo{
		Host:     hostname,
		UserName: username,
		Path:     filepath.Clean(absPath),
	}, nil
}

// HashString generates hash of SourceInfo.
func (ssi SourceInfo) HashString() string {
	h := sha1.New()
	io.WriteString(h, ssi.Host)
	h.Write(zeroByte)
	io.WriteString(h, ssi.UserName)
	h.Write(zeroByte)
	io.WriteString(h, ssi.Path)
	h.Write(zeroByte)
	return hex.EncodeToString(h.Sum(nil))
}
