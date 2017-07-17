package cli

import (
	"fmt"
	"strings"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/repofs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/vault"
)

// ParseObjectID interprets the given ID string and returns corresponding repo.ObjectID.
func parseObjectID(id string, vlt *vault.Vault, r *repo.Repository) (repo.ObjectID, error) {
	head, tail := splitHeadTail(id)
	if len(head) == 0 {
		return repo.NullObjectID, fmt.Errorf("invalid object ID: %v", id)
	}

	if !strings.HasPrefix(id, vault.StoredObjectIDPrefix) {
		return repo.ParseObjectID(id)
	}

	oid, err := vlt.GetObjectID(head)
	if err != nil {
		return repo.NullObjectID, fmt.Errorf("can't retrieve vault object ID %v: %v", head, err)
	}

	if tail == "" {
		return oid, nil
	}

	dir := repofs.Directory(r, oid)
	if err != nil {
		return repo.NullObjectID, err
	}

	return parseNestedObjectID(dir, tail)
}

func parseNestedObjectID(startingDir fs.Directory, id string) (repo.ObjectID, error) {
	head, tail := splitHeadTail(id)
	var current fs.Entry
	current = startingDir
	for head != "" {
		dir, ok := current.(fs.Directory)
		if !ok {
			return repo.NullObjectID, fmt.Errorf("entry not found '%v': parent is not a directory", head)
		}

		entries, err := dir.Readdir()
		if err != nil {
			return repo.NullObjectID, err
		}

		e := entries.FindByName(head)
		if e == nil {
			return repo.NullObjectID, fmt.Errorf("entry not found: '%v'", head)
		}

		current = e
		head, tail = splitHeadTail(tail)
	}

	return current.(repo.HasObjectID).ObjectID(), nil
}

func splitHeadTail(id string) (string, string) {
	p := strings.Index(id, "/")
	if p < 0 {
		return id, ""
	}

	return id[:p], id[p+1:]
}
