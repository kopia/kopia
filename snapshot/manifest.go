package snapshot

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// Manifest represents information about a single point-in-time filesystem snapshot.
type Manifest struct {
	ID     manifest.ID `json:"id"`
	Source SourceInfo  `json:"source"`

	Description string          `json:"description"`
	StartTime   fs.UTCTimestamp `json:"startTime"`
	EndTime     fs.UTCTimestamp `json:"endTime"`

	Stats            Stats  `json:"stats,omitempty"`
	IncompleteReason string `json:"incomplete,omitempty"`

	RootEntry *DirEntry `json:"rootEntry"`

	RetentionReasons []string `json:"-"`

	Tags map[string]string `json:"tags,omitempty"`

	// usage, not persisted, values depend on walking snapshot list in a particular order
	StorageStats *StorageStats `json:"storageStats,omitempty"`

	// list of manually-defined pins which prevent the snapshot from being deleted.
	Pins []string `json:"pins,omitempty"`
}

// UpdatePins updates pins in the provided manifest.
func (m *Manifest) UpdatePins(add, remove []string) bool {
	newPins := map[string]bool{}
	changed := false

	for _, r := range m.Pins {
		newPins[r] = true
	}

	for _, r := range add {
		if !newPins[r] {
			newPins[r] = true
			changed = true
		}
	}

	for _, r := range remove {
		if newPins[r] {
			delete(newPins, r)

			changed = true
		}
	}

	m.Pins = nil
	for r := range newPins {
		m.Pins = append(m.Pins, r)
	}

	sort.Strings(m.Pins)

	return changed
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
//
//nolint:recvcheck
type Permissions int

// MarshalJSON emits permissions as octal string.
func (p Permissions) MarshalJSON() ([]byte, error) {
	if p == 0 {
		return nil, nil
	}

	s := "0" + strconv.FormatInt(int64(p), 8)

	//nolint:wrapcheck
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
	ModTime     fs.UTCTimestamp      `json:"mtime,omitempty"`
	UserID      uint32               `json:"uid,omitempty"`
	GroupID     uint32               `json:"gid,omitempty"`
	ObjectID    object.ID            `json:"obj,omitempty"`
	DirSummary  *fs.DirectorySummary `json:"summ,omitempty"`
}

// Clone returns a clone of the entry.
func (e *DirEntry) Clone() *DirEntry {
	e2 := *e

	if s := e2.DirSummary; s != nil {
		s2 := s.Clone()

		e2.DirSummary = &s2
	}

	return &e2
}

// HasDirEntry is implemented by objects that have a DirEntry associated with them.
type HasDirEntry interface {
	DirEntry() *DirEntry
}

// HasDirEntryOrNil is implemented by objects that may have a DirEntry
// stored in the object's corresponding shallow placeholder file.
type HasDirEntryOrNil interface {
	DirEntryOrNil(ctx context.Context) (*DirEntry, error)
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

	return object.EmptyID
}

// Clone returns a clone of the manifest.
func (m *Manifest) Clone() *Manifest {
	m2 := *m

	if m2.RootEntry != nil {
		m2.RootEntry = m2.RootEntry.Clone()
	}

	return &m2
}

// StorageStats encapsulates snapshot storage usage information and running totals.
type StorageStats struct {
	// amount of new unique data in this snapshot that wasn't there before.
	// note that this depends on ordering of snapshots.
	NewData      StorageUsageDetails `json:"newData,omitempty"`
	RunningTotal StorageUsageDetails `json:"runningTotal,omitempty"`
}

// StorageUsageDetails provides details about snapshot storage usage.
type StorageUsageDetails struct {
	// number of bytes in all objects (ignoring content-level deduplication).
	// +checkatomic
	ObjectBytes int64 `json:"objectBytes"`

	// number of bytes in all unique contents (original).
	// +checkatomic
	OriginalContentBytes int64 `json:"originalContentBytes"`

	// number of bytes in all unique contents as stored in the repository.
	// +checkatomic
	PackedContentBytes int64 `json:"packedContentBytes"`

	// number of unique file objects.
	// +checkatomic
	FileObjectCount int32 `json:"fileObjects"`

	// number of unique objects.
	// +checkatomic
	DirObjectCount int32 `json:"dirObjects"`

	// number of unique contents.
	// +checkatomic
	ContentCount int32 `json:"contents"`
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
		return result[i].StartTime > result[j].StartTime == reverse
	})

	return result
}
