package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/repofs"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/repo"
)

// ParseObjectID interprets the given ID string and returns corresponding object.ID.
func parseObjectID(ctx context.Context, rep *repo.Repository, id string) (object.ID, error) {
	parts := strings.Split(id, "/")
	oid, err := object.ParseID(parts[0])
	if err != nil {
		return "", fmt.Errorf("can't parse object ID %v: %v", id, err)
	}

	if len(parts) == 1 {
		return oid, nil
	}

	dir := repofs.DirectoryEntry(rep, oid, nil)
	return parseNestedObjectID(ctx, dir, parts[1:])
}

func getNestedEntry(ctx context.Context, startingDir fs.Entry, parts []string) (fs.Entry, error) {
	current := startingDir
	for _, part := range parts {
		if part == "" {
			continue
		}
		dir, ok := current.(fs.Directory)
		if !ok {
			return nil, fmt.Errorf("entry not found %q: parent is not a directory", part)
		}

		entries, err := dir.Readdir(ctx)
		if err != nil {
			return nil, err
		}

		e := entries.FindByName(part)
		if e == nil {
			return nil, fmt.Errorf("entry not found: %q", part)
		}

		current = e
	}

	return current, nil
}

func parseNestedObjectID(ctx context.Context, startingDir fs.Entry, parts []string) (object.ID, error) {
	e, err := getNestedEntry(ctx, startingDir, parts)
	if err != nil {
		return "", err
	}
	return e.(object.HasObjectID).ObjectID(), nil
}
