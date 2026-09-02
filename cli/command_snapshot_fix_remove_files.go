package cli

import (
	"context"
	"path"
	"slices"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

type commandSnapshotFixRemoveFiles struct {
	common commonRewriteSnapshots

	removeObjectIDs   []string
	removeFilesByName []string
}

func (c *commandSnapshotFixRemoveFiles) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("remove-files", "Remove references to the specified files from snapshots.")
	c.common.setup(svc, cmd)

	cmd.Flag("object-id", "Remove files by their object ID").StringsVar(&c.removeObjectIDs)
	cmd.Flag("filename", "Remove files by filename (wildcards are supported)").StringsVar(&c.removeFilesByName)

	cmd.Action(svc.repositoryWriterActionWithMaintenance(c.run))
}

func (c *commandSnapshotFixRemoveFiles) rewriteEntry(ctx context.Context, pathFromRoot string, ent *snapshot.DirEntry) (*snapshot.DirEntry, error) {
	if slices.Contains(c.removeObjectIDs, ent.ObjectID.String()) {
		log(ctx).Infof("will remove file %v", pathFromRoot)

		return nil, nil
	}

	// pathFromRoot is already the entry's full path relative to the snapshot root.
	for _, n := range c.removeFilesByName {
		matched, err := matchPathPattern(n, pathFromRoot)
		if !matched && err == nil {
			matched, err = path.Match(n, ent.Name)
		}
		if err != nil {
			return nil, errors.Wrap(err, "invalid wildcard")
		}

		if matched {
			log(ctx).Infof("will remove file %v", pathFromRoot)

			return nil, nil
		}
	}

	return ent, nil
}

// matchPathPattern reports whether glob pattern matches the full path.
//
// The pattern must match a suffix of the path: it may start at any depth, but
// from there each pattern segment must match exactly one consecutive path
// segment using path.Match wildcards. Pattern segments may not be skipped and
// a "*" spans exactly one path segment.
//
// A single-segment pattern such as ".vscode" matches by basename at any depth,
// preserving the original behavior, while patterns such as "Users/liquid/.vscode"
// or "*/.vscode" match by anchored path suffix.
func matchPathPattern(pattern, full string) (bool, error) {
	patternSegments := strings.Split(pattern, "/")
	pathSegments := strings.Split(full, "/")

	// the pattern can only match if it is no longer than the path
	if len(patternSegments) > len(pathSegments) {
		return false, nil
	}

	for start := 0; start <= len(pathSegments)-len(patternSegments); start++ {
		matchedAll := true

		for i, seg := range patternSegments {
			ok, err := path.Match(seg, pathSegments[start+i])
			if err != nil {
				return false, errors.Wrap(err, "invalid wildcard")
			}
			if !ok {
				matchedAll = false
				break
			}
		}

		if matchedAll {
			return true, nil
		}
	}

	return false, nil
}

func (c *commandSnapshotFixRemoveFiles) run(ctx context.Context, rep repo.RepositoryWriter) error {
	if len(c.removeObjectIDs)+len(c.removeFilesByName) == 0 {
		return errors.New("must specify files to remove")
	}

	return c.common.rewriteMatchingSnapshots(ctx, rep, c.rewriteEntry)
}
