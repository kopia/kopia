package cli

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type commandSnapshotSearch struct {
	pattern           string
	ignoreCase        bool
	includeIncomplete bool
	snapshotIDs       []string
	maxResults        int
	jo                jsonOutput
	out               textOutput
}

type snapshotSearchResult struct {
	SnapshotID manifest.ID         `json:"snapshotID"`
	Source     snapshot.SourceInfo `json:"source"`
	StartTime  fs.UTCTimestamp     `json:"startTime"`
	Path       string              `json:"path"`
	Type       string              `json:"type"`
	ObjectID   object.ID           `json:"objectID"`
	Size       int64               `json:"size"`
}

var errSnapshotSearchLimit = errors.New("snapshot search result limit reached")

func (c *commandSnapshotSearch) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("search", "Search entry names across snapshots without restoring file contents.")
	cmd.Arg("pattern", "Literal substring to find in file and directory names.").Required().StringVar(&c.pattern)
	cmd.Flag("ignore-case", "Match names without regard to case.").BoolVar(&c.ignoreCase)
	cmd.Flag("snapshot", "Search only this snapshot ID (repeatable). Defaults to all visible snapshots.").StringsVar(&c.snapshotIDs)
	cmd.Flag("incomplete", "Include incomplete snapshots.").BoolVar(&c.includeIncomplete)
	cmd.Flag("max-results", "Stop after this many matches; 0 means unlimited.").IntVar(&c.maxResults)
	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandSnapshotSearch) loadSnapshots(ctx context.Context, rep repo.Repository) ([]*snapshot.Manifest, error) {
	var ids []manifest.ID

	for _, id := range c.snapshotIDs {
		ids = append(ids, manifest.ID(id))
	}

	if len(ids) == 0 {
		var err error

		ids, err = snapshot.ListSnapshotManifests(ctx, rep, nil, nil)
		if err != nil {
			return nil, errors.Wrap(err, "listing snapshots to search")
		}
	}

	seen := map[manifest.ID]bool{}

	var manifests []*snapshot.Manifest

	for _, id := range ids {
		if seen[id] {
			continue
		}

		seen[id] = true

		m, err := snapshot.LoadSnapshot(ctx, rep, id)
		if err != nil {
			return nil, errors.Wrapf(err, "loading snapshot %v", id)
		}

		if m.IncompleteReason == "" || c.includeIncomplete {
			manifests = append(manifests, m)
		}
	}

	slices.SortFunc(manifests, func(a, b *snapshot.Manifest) int {
		if a.StartTime != b.StartTime {
			return cmp.Compare(b.StartTime, a.StartTime)
		}

		return cmp.Compare(a.ID, b.ID)
	})

	return manifests, nil
}

func (c *commandSnapshotSearch) run(ctx context.Context, rep repo.Repository) error {
	if c.maxResults < 0 {
		return errors.New("max-results must not be negative")
	}

	searcher, err := snapshotfs.NewNameSearcher(c.pattern, c.ignoreCase)
	if err != nil {
		return errors.Wrap(err, "invalid name search")
	}

	manifests, err := c.loadSnapshots(ctx, rep)
	if err != nil {
		return err
	}

	var (
		jl    jsonList
		count int
	)

	jl.begin(&c.jo)
	defer jl.end()

	for _, m := range manifests {
		err := searchSnapshotNames(ctx, rep, searcher, m, func(match snapshotfs.NameMatch) error {
			if err := c.outputMatch(&jl, m, match); err != nil {
				return err
			}

			count++

			if c.maxResults > 0 && count >= c.maxResults {
				return errSnapshotSearchLimit
			}

			return nil
		})
		if errors.Is(err, errSnapshotSearchLimit) {
			return nil
		}

		if err != nil {
			return errors.Wrapf(err, "searching snapshot %v", m.ID)
		}
	}

	return nil
}

func searchSnapshotNames(ctx context.Context, rep repo.Repository, searcher *snapshotfs.NameSearcher, m *snapshot.Manifest, visit func(snapshotfs.NameMatch) error) error {
	root, err := snapshotfs.SnapshotRoot(rep, m)
	if err != nil {
		return errors.Wrap(err, "opening snapshot root")
	}
	defer root.Close()

	return errors.Wrap(searcher.Search(ctx, root, visit), "searching entry names")
}

func (c *commandSnapshotSearch) outputMatch(jl *jsonList, m *snapshot.Manifest, match snapshotfs.NameMatch) error {
	typ := "file"

	switch {
	case match.Mode.IsDir():
		typ = "directory"
	case match.Mode&os.ModeSymlink != 0:
		typ = "symlink"
	}

	if c.jo.jsonOutput {
		jl.emit(snapshotSearchResult{
			SnapshotID: m.ID, Source: m.Source, StartTime: m.StartTime,
			Path: match.Path, Type: typ, ObjectID: match.ObjectID, Size: match.Size,
		})

		return nil
	}

	_, err := fmt.Fprintf(c.out.stdout(), "%v %v %q\n", m.ID, m.Source, match.Path)

	return errors.Wrap(err, "writing search result")
}
