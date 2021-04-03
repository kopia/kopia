package snapshot

import (
	"encoding/json"
	"sort"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// Manifest represents information about a single point-in-time filesystem snapshot.
type Manifest struct {
	ID     manifest.ID `json:"id"`
	Source SourceInfo  `json:"source"`

	Description string    `json:"description"`
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`

	Stats            Stats  `json:"stats,omitempty"`
	IncompleteReason string `json:"incomplete,omitempty"`

	RootEntry *DirEntry `json:"rootEntry"`

	RetentionReasons []string `json:"-"`
}

// EntryType is a type of a filesystem entry.
type EntryType string

// Supported entry types.
const (
	EntryTypeUnknown   EntryType = ""  // unknown type
	EntryTypeFile      EntryType = "f" // file
	EntryTypeDirectory EntryType = "d" // directory
	EntryTypeSymlink   EntryType = "s" // symbolic link
)

// Permissions encapsulates UNIX permissions for a filesystem entry.
type Permissions int

// MarshalJSON emits permissions as octal string.
func (p Permissions) MarshalJSON() ([]byte, error) {
	if p == 0 {
		return nil, nil
	}

	s := "0" + strconv.FormatInt(int64(p), 8)

	return json.Marshal(&s)
}

// UnmarshalJSON parses octal permissions string from JSON.
func (p *Permissions) UnmarshalJSON(b []byte) error {
	var s string

	if err := json.Unmarshal(b, &s); err != nil {
		return errors.Wrap(err, "unable to unmarshal JSON")
	}

	v, err := strconv.ParseInt(s, 0, 32)
	if err != nil {
		return errors.Wrap(err, "unable to parse permission string")
	}

	*p = Permissions(v)

	return nil
}

// DirEntry represents a directory entry as stored in JSON stream.
type DirEntry struct {
	Name        string               `json:"name,omitempty"`
	Type        EntryType            `json:"type,omitempty"`
	Permissions Permissions          `json:"mode,omitempty"`
	FileSize    int64                `json:"size,omitempty"`
	ModTime     time.Time            `json:"mtime,omitempty"`
	UserID      uint32               `json:"uid,omitempty"`
	GroupID     uint32               `json:"gid,omitempty"`
	ObjectID    object.ID            `json:"obj,omitempty"`
	DirSummary  *fs.DirectorySummary `json:"summ,omitempty"`
}

// HasDirEntry is implemented by objects that have a DirEntry associated with them.
type HasDirEntry interface {
	DirEntry() *DirEntry
}

// DirManifest represents serialized contents of a directory.
// The entries are sorted lexicographically and summary only refers to properties of
// entries, so directory with the same contents always serializes to exactly the same JSON.
type DirManifest struct {
	StreamType string               `json:"stream"` // legacy
	Entries    []*DirEntry          `json:"entries"`
	Summary    *fs.DirectorySummary `json:"summary"`
}

// RootObjectID returns the ID of a root object.
func (m *Manifest) RootObjectID() object.ID {
	if m.RootEntry != nil {
		return m.RootEntry.ObjectID
	}

	return ""
}

// GroupBySource returns a slice of slices, such that each result item contains manifests from a single source.
func GroupBySource(manifests []*Manifest) [][]*Manifest {
	resultMap := map[SourceInfo][]*Manifest{}
	for _, m := range manifests {
		resultMap[m.Source] = append(resultMap[m.Source], m)
	}

	var result [][]*Manifest
	for _, v := range resultMap {
		result = append(result, v)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i][0].Source.String() < result[j][0].Source.String()
	})

	return result
}

// SortByTime returns a slice of manifests sorted by start time.
func SortByTime(manifests []*Manifest, reverse bool) []*Manifest {
	result := append([]*Manifest(nil), manifests...)
	sort.Slice(result, func(i, j int) bool {
		return result[i].StartTime.After(result[j].StartTime) == reverse
	})

	return result
}
