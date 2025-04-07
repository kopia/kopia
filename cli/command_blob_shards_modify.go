package cli

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
)

type commandBlobShardsModify struct {
	rootPath         string
	defaultShardSpec string
	overrideSpecs    []string
	removeOverrides  []string
	dryRun           bool
	unshardedLength  int

	out textOutput
}

func (c *commandBlobShardsModify) setup(svc appServices, parent commandParent) {
	c.unshardedLength = -1

	cmd := parent.Command("modify", "Perform low-level resharding of blob storage").Hidden().Alias("reshard")
	cmd.Flag("i-am-sure-kopia-is-not-running", "Confirm that no other instance of kopia is running").Required().Bool()
	cmd.Flag("path", "Sharded directory path").Required().ExistingDirVar(&c.rootPath)
	cmd.Flag("default-shards", "Default specification 'n1,..nN' or 'flat')").StringVar(&c.defaultShardSpec)
	cmd.Flag("override", "Override specification 'prefix=n1,..nN')").StringsVar(&c.overrideSpecs)
	cmd.Flag("remove-override", "Override specification 'prefix=n1,..nN')").StringsVar(&c.removeOverrides)
	cmd.Flag("unsharded-length", "Minimum sharded length").IntVar(&c.unshardedLength)
	cmd.Flag("dry-run", "Dry run").BoolVar(&c.dryRun)
	cmd.Action(svc.noRepositoryAction(c.run))

	c.out.setup(svc)
}

func (c *commandBlobShardsModify) getParameters(dotShardsFile string) (*sharded.Parameters, error) {
	//nolint:gosec
	f, err := os.Open(dotShardsFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open shards file")
	}

	p := &sharded.Parameters{}
	if err := p.Load(f); err != nil {
		return nil, errors.Wrap(err, "error loading parameters")
	}

	return p, nil
}

func parseShardSpec(shards string) ([]int, error) {
	result := []int{}

	if shards == "flat" {
		return result, nil
	}

	parts := strings.Split(shards, ",")

	for _, p := range parts {
		if p == "" {
			continue
		}

		v, err := strconv.Atoi(p)
		if err != nil || v < 0 {
			return nil, errors.New("invalid shard specification")
		}

		result = append(result, v)
	}

	return result, nil
}

func prefixAndShardsWithout(pas []sharded.PrefixAndShards, without blob.ID) []sharded.PrefixAndShards {
	result := []sharded.PrefixAndShards{}

	for _, it := range pas {
		if it.Prefix != without {
			result = append(result, it)
		}
	}

	return result
}

func (c *commandBlobShardsModify) applyParameterChangesFromFlags(p *sharded.Parameters) error {
	if c.defaultShardSpec != "" {
		v, err := parseShardSpec(c.defaultShardSpec)
		if err != nil {
			return errors.New("invalid --default-shards")
		}

		p.DefaultShards = v
	}

	for _, ov := range c.removeOverrides {
		p.Overrides = prefixAndShardsWithout(p.Overrides, blob.ID(ov))
	}

	for _, ov := range c.overrideSpecs {
		parts := strings.Split(ov, "=")
		if len(parts) <= 1 {
			return errors.Errorf("invalid override %q, must be prefix=n1,..,nM", ov)
		}

		v, err := parseShardSpec(parts[1])
		if err != nil {
			return errors.Errorf("invalid override %q, must be prefix=n1,..,nM", ov)
		}

		p.Overrides = append(
			prefixAndShardsWithout(p.Overrides, blob.ID(parts[0])),
			sharded.PrefixAndShards{
				Prefix: blob.ID(parts[0]),
				Shards: v,
			})
	}

	if c.unshardedLength != -1 {
		p.UnshardedLength = c.unshardedLength
	}

	return nil
}

func (c *commandBlobShardsModify) run(ctx context.Context) error {
	var numMoved, numUnchanged, numRemoved int

	dotShardsFile := filepath.Join(c.rootPath, sharded.ParametersFile)

	log(ctx).Info("Reading .shards file.")

	srcPar, err := c.getParameters(dotShardsFile)
	if err != nil {
		return err
	}

	dstPar := srcPar.Clone()

	if err2 := c.applyParameterChangesFromFlags(dstPar); err2 != nil {
		return err2
	}

	log(ctx).Info("Moving files...")

	if err2 := c.renameBlobs(ctx, c.rootPath, "", dstPar, &numMoved, &numUnchanged); err2 != nil {
		return errors.Wrap(err2, "error processing directory")
	}

	if c.dryRun {
		log(ctx).Infof("Would move %v file, %v unchanged.", numMoved, numUnchanged)

		return nil
	}

	log(ctx).Infof("Moved %v files, %v unchanged.", numMoved, numUnchanged)
	log(ctx).Info("Removing empty directories...")

	if _, err2 := c.removeEmptyDirs(ctx, c.rootPath, &numRemoved); err2 != nil {
		return errors.Wrap(err2, "error removing empty directories")
	}

	log(ctx).Infof("Removed %v empty directories...", numRemoved)
	log(ctx).Info("Writing new .shards file.")

	of, err := os.Create(dotShardsFile) //nolint:gosec
	if err != nil {
		return errors.Wrap(err, "error creating .shards file")
	}
	defer of.Close() //nolint:errcheck

	return errors.Wrap(dstPar.Save(of), "error saving .shards file")
}

func (c *commandBlobShardsModify) removeEmptyDirs(ctx context.Context, dir string, numRemoved *int) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, errors.Wrap(err, "error reading directory")
	}

	isEmpty := true

	for _, ent := range entries {
		//nolint:nestif
		if ent.IsDir() {
			childPath := path.Join(dir, ent.Name())

			subDirEmpty, err := c.removeEmptyDirs(ctx, childPath, numRemoved)
			if err != nil {
				return false, err
			}

			if !subDirEmpty {
				isEmpty = false
			} else {
				c.out.printStdout("rmdir %v\n", childPath)

				*numRemoved++

				if !c.dryRun {
					if err := os.Remove(childPath); err != nil {
						log(ctx).Errorf("Unable to remove directory %v", childPath)

						isEmpty = false
					}
				}
			}
		} else {
			isEmpty = false
		}
	}

	return isEmpty, nil
}

func (c *commandBlobShardsModify) renameBlobs(ctx context.Context, dir, prefix string, params *sharded.Parameters, numMoved, numUnchanged *int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.Wrap(err, "error reading directory")
	}

	for _, ent := range entries {
		//nolint:nestif
		if ent.IsDir() {
			if err := c.renameBlobs(ctx, path.Join(dir, ent.Name()), prefix+ent.Name(), params, numMoved, numUnchanged); err != nil {
				return err
			}
		} else if strings.HasSuffix(ent.Name(), sharded.CompleteBlobSuffix) {
			blobID := prefix + strings.TrimSuffix(ent.Name(), sharded.CompleteBlobSuffix)

			destDir, destBlobID := params.GetShardDirectoryAndBlob(c.rootPath, blob.ID(blobID))
			srcFile := path.Join(dir, ent.Name())
			destFile := fmt.Sprintf("%v/%v%v", destDir, destBlobID, sharded.CompleteBlobSuffix)

			if srcFile == destFile {
				log(ctx).Debugf("Unchanged: %v", srcFile)

				*numUnchanged++
			} else {
				c.out.printStdout("mv %v %v\n", srcFile, destFile)

				if !c.dryRun {
					err := os.Rename(srcFile, destFile)
					if os.IsNotExist(err) {
						//nolint:mnd
						if err2 := os.MkdirAll(destDir, 0o700); err2 != nil {
							return errors.Wrap(err2, "error creating directory")
						}

						err = os.Rename(srcFile, destFile)
					}

					if err != nil {
						return errors.Wrap(err, "error moving")
					}
				}

				*numMoved++
			}
		}
	}

	return nil
}
