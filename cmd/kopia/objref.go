package main

import (
	"fmt"
	"strings"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/vault"
)

// ParseObjectID interprets the given ID string and returns corresponding repo.ObjectID.
// The string can be:
// - backupID (vxxxxxx/12312312)
// -
func ParseObjectID(id string, vlt vault.Reader) (repo.ObjectID, error) {
	head, tail := splitHeadTail(id)
	if len(head) == 0 {
		return "", fmt.Errorf("invalid object ID: %v", id)
	}

	oid, err := vlt.ResolveObjectID(head)
	if err != nil {
		return "", err
	}

	if tail == "" {
		return oid, nil
	}

	r, err := vlt.OpenRepository()
	if err != nil {
		return "", fmt.Errorf("cannot open repository: %v", err)
	}

	return parseNestedObjectID(oid, tail, r)
}

func parseNestedObjectID(current repo.ObjectID, id string, r repo.Repository) (repo.ObjectID, error) {
	head, tail := splitHeadTail(id)
	for head != "" {
		d, err := r.Open(current)
		if err != nil {
			return "", err
		}

		dir, err := fs.ReadDirectory(d, "")
		if err != nil {
			return "", fmt.Errorf("entry not found '%v': parent is not a directory", head)
		}

		e := dir.FindByName(head)
		if e == nil {
			return "", fmt.Errorf("entry not found: '%v'", head)
		}

		current = e.ObjectID

		head, tail = splitHeadTail(tail)
	}

	return current, nil
}

func splitHeadTail(id string) (string, string) {
	p := strings.Index(id, "/")
	if p < 0 {
		return id, ""
	}

	return id[:p], id[p+1:]
}
