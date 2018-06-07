package cli

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	verifyCommand               = objectCommands.Command("verify", "Verify the contents of stored object")
	verifyCommandErrorThreshold = verifyCommand.Flag("max-errors", "Maximum number of errors before stopping").Default("0").Int()
	verifyCommandDirObjectIDs   = verifyCommand.Flag("directory-id", "Directory object IDs to verify").Strings()
	verifyCommandFileObjectIDs  = verifyCommand.Flag("file-id", "File object IDs to verify").Strings()
	verifyCommandAllSources     = verifyCommand.Flag("all-sources", "Verify all snapshots").Bool()
	verifyCommandSources        = verifyCommand.Flag("sources", "Verify the provided sources").Strings()
)

type verifier struct {
	mgr     *snapshot.Manager
	om      *object.Manager
	visited map[string]bool
	errors  []error
}

func (v *verifier) tooManyErrors() bool {
	if *verifyCommandErrorThreshold == 0 {
		return false
	}

	return len(v.errors) >= *verifyCommandErrorThreshold
}

func (v *verifier) reportError(path string, err error) bool {
	log.Warn().Str("path", path).Err(err).Msg("failed")
	v.errors = append(v.errors, err)
	return len(v.errors) >= *verifyCommandErrorThreshold
}

func (v *verifier) verifyDirectory(ctx context.Context, oid object.ID, path string) {
	if v.visited[oid.String()] {
		return
	}
	v.visited[oid.String()] = true

	log.Printf("verifying directory %q (%v)", path, oid)

	d := v.mgr.DirectoryEntry(oid, nil)
	entries, err := d.Readdir(ctx)
	if err != nil {
		v.reportError(path, fmt.Errorf("error reading %v: %v", oid, err))
		return
	}

	for _, e := range entries {
		if v.tooManyErrors() {
			break
		}

		m := e.Metadata()
		objectID := e.(object.HasObjectID).ObjectID()
		childPath := path + "/" + m.Name
		if m.FileMode().IsDir() {
			v.verifyDirectory(ctx, objectID, childPath)
		} else {
			v.verifyObject(ctx, objectID, childPath, m.FileSize)
		}
	}
}

func (v *verifier) verifyObject(ctx context.Context, oid object.ID, path string, expectedLength int64) {
	if v.visited[oid.String()] {
		return
	}
	v.visited[oid.String()] = true

	if expectedLength < 0 {
		log.Printf("verifying object %v", oid)
	} else {
		log.Printf("verifying object %v (%v) with length %v", path, oid, expectedLength)
	}

	length, _, err := v.om.VerifyObject(ctx, oid)
	if err != nil {
		v.reportError(path, fmt.Errorf("error verifying %v: %v", oid, err))
	}

	if expectedLength >= 0 && length != expectedLength {
		v.reportError(path, fmt.Errorf("invalid object length %q, %v, expected %v", oid, length, expectedLength))
	}
}

func runVerifyCommand(ctx context.Context, rep *repo.Repository) error {
	mgr := snapshot.NewManager(rep)

	v := verifier{
		mgr,
		rep.Objects,
		make(map[string]bool),
		nil,
	}

	if *verifyCommandAllSources || len(*verifyCommandSources) > 0 {
		var manifestIDs []string
		if *verifyCommandAllSources {
			manifestIDs = append(manifestIDs, mgr.ListSnapshotManifests(nil)...)
		} else {
			for _, srcStr := range *verifyCommandSources {
				src, err := snapshot.ParseSourceInfo(srcStr, getHostName(), getUserName())
				if err != nil {
					return fmt.Errorf("error parsing %q: %v", srcStr, err)
				}
				manifestIDs = append(manifestIDs, mgr.ListSnapshotManifests(&src)...)
			}
		}
		manifests, err := mgr.LoadSnapshots(manifestIDs)
		if err != nil {
			return err
		}
		for _, man := range manifests {
			path := fmt.Sprintf("%v@%v", man.Source, man.StartTime.Format(timeFormat))
			if man.RootEntry != nil {
				if man.RootEntry.Type == fs.EntryTypeDirectory {
					v.verifyDirectory(ctx, man.RootObjectID(), path)
				} else {
					v.verifyObject(ctx, man.RootObjectID(), path, -1)
				}
			}
		}
	}

	for _, oidStr := range *verifyCommandDirObjectIDs {
		oid, err := parseObjectID(ctx, mgr, oidStr)
		if err != nil {
			return err
		}

		v.verifyDirectory(ctx, oid, oidStr)
	}

	for _, oidStr := range *verifyCommandFileObjectIDs {
		oid, err := parseObjectID(ctx, mgr, oidStr)
		if err != nil {
			return err
		}

		v.verifyObject(ctx, oid, oidStr, -1)
	}

	if len(v.errors) == 0 {
		return nil
	}

	return fmt.Errorf("encountered %v errors", len(v.errors))
}

func init() {
	verifyCommand.Action(repositoryAction(runVerifyCommand))
}
