package cli

import (
	"fmt"
	"strings"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/snapshot"
)

// ParseObjectID interprets the given ID string and returns corresponding object.ObjectID.
func parseObjectID(mgr *snapshot.Manager, id string) (object.ID, error) {
	head, tail := splitHeadTail(id)
	if len(head) == 0 {
		return object.NullObjectID, fmt.Errorf("invalid object ID: %v", id)
	}

	oid, err := object.ParseObjectID(head)
	if err != nil {
		return object.NullObjectID, fmt.Errorf("can't parse object ID %v: %v", head, err)
	}

	if tail == "" {
		return oid, nil
	}

	dir := mgr.DirectoryEntry(oid)
	if err != nil {
		return object.NullObjectID, err
	}

	return parseNestedObjectID(dir, tail)
}

func parseNestedObjectID(startingDir fs.Directory, id string) (object.ID, error) {
	head, tail := splitHeadTail(id)
	var current fs.Entry
	current = startingDir
	for head != "" {
		dir, ok := current.(fs.Directory)
		if !ok {
			return object.NullObjectID, fmt.Errorf("entry not found '%v': parent is not a directory", head)
		}

		entries, err := dir.Readdir()
		if err != nil {
			return object.NullObjectID, err
		}

		e := entries.FindByName(head)
		if e == nil {
			return object.NullObjectID, fmt.Errorf("entry not found: '%v'", head)
		}

		current = e
		head, tail = splitHeadTail(tail)
	}

	return current.(object.HasObjectID).ObjectID(), nil
}

func splitHeadTail(id string) (string, string) {
	p := strings.Index(id, "/")
	if p < 0 {
		return id, ""
	}

	return id[:p], id[p+1:]
}
