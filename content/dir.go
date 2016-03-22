package content

import "time"

// Entry stores attributes of a single entry in a directory.
type Entry struct {
	Name     string
	Size     int64
	Type     EntryType
	ModTime  time.Time
	Mode     int16 // 0000 .. 0777
	UserID   uint32
	GroupID  uint32
	ObjectID ObjectID
}

// Directory contains access to contents of directory, both in original order and indexed by name.
type Directory struct {
	Ordered []*Entry
	ByName  map[string]*Entry
}
