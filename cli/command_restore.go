package cli

import (
	"archive/zip"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/restore"
)

const (
	restoreCommandHelp = `Restore a directory from a snapshot into the specified target path.

By default, the target path will be created by the restore command if it does
not exist.

The source to be restored is specified in the form of a directory ID and
optionally a sub-directory path.

For example, the following source and target arguments will restore the contents
of the 'kffbb7c28ea6c34d6cbe555d1cf80faa9' directory into a new, local directory
named 'd1'

'restore kffbb7c28ea6c34d6cbe555d1cf80faa9 d1'

Similarly, the following command will restore the contents of a subdirectory
'subdir/subdir2' under 'kffbb7c28ea6c34d6cbe555d1cf80faa9'  into a new, local
directory named 'sd2'

'restore kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2 sd2'
`
	restoreCommandSourcePathHelp = `Source directory ID/path in the form of a
directory ID and optionally a sub-directory path. For example,
'kffbb7c28ea6c34d6cbe555d1cf80faa9' or
'kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2'
`
)

var (
	restoreCommand           = app.Command("restore", restoreCommandHelp)
	restoreCommandSourcePath = restoreCommand.Arg("source-path", restoreCommandSourcePathHelp).Required().String()

	restoreTargetPath           = ""
	restoreOverwriteDirectories = true
	restoreOverwriteFiles       = true
	restoreMode                 = restoreModeAuto
)

const (
	restoreModeLocal         = "local"
	restoreModeAuto          = "auto"
	restoreModeZip           = "zip"
	restoreModeZipNoCompress = "zip-nocompress"
	restoreModeTar           = "tar"
	restoreModeTgz           = "tgz"
)

func addRestoreFlags(cmd *kingpin.CmdClause) {
	cmd.Arg("target-path", "Path of the directory for the contents to be restored").Required().StringVar(&restoreTargetPath)
	cmd.Flag("overwrite-directories", "Overwrite existing directories").BoolVar(&restoreOverwriteDirectories)
	cmd.Flag("overwrite-files", "Specifies whether or not to overwrite already existing files").BoolVar(&restoreOverwriteFiles)
	cmd.Flag("mode", "Override restore mode").EnumVar(&restoreMode, restoreModeAuto, restoreModeLocal, restoreModeZip, restoreModeZipNoCompress, restoreModeTar, restoreModeTgz)
}

func restoreOutput() (restore.Output, error) {
	p, err := filepath.Abs(restoreTargetPath)
	if err != nil {
		return nil, err
	}

	m := detectRestoreMode(restoreMode)
	switch m {
	case restoreModeLocal:
		return &restore.FilesystemOutput{
			TargetPath:           p,
			OverwriteDirectories: restoreOverwriteDirectories,
			OverwriteFiles:       restoreOverwriteFiles,
		}, nil

	case restoreModeZip, restoreModeZipNoCompress:
		f, err := os.Create(restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		method := zip.Deflate
		if m == restoreModeZipNoCompress {
			method = zip.Store
		}

		return restore.NewZipOutput(f, method), nil

	case restoreModeTar:
		f, err := os.Create(restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		return restore.NewTarOutput(f), nil

	case restoreModeTgz:
		f, err := os.Create(restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		return restore.NewTarOutput(gzip.NewWriter(f)), nil

	default:
		return nil, errors.Errorf("unknown mode %v", m)
	}
}

func detectRestoreMode(m string) string {
	if m != "auto" {
		return m
	}

	switch {
	case strings.HasSuffix(restoreTargetPath, ".zip"):
		printStderr("Restoring to a zip file (%v)...\n", restoreTargetPath)
		return restoreModeZip

	case strings.HasSuffix(restoreTargetPath, ".tar"):
		printStderr("Restoring to an uncompressed tar file (%v)...\n", restoreTargetPath)
		return restoreModeTar

	case strings.HasSuffix(restoreTargetPath, ".tar.gz") || strings.HasSuffix(restoreTargetPath, ".tgz"):
		printStderr("Restoring to a tar+gzip file (%v)...\n", restoreTargetPath)
		return restoreModeTgz

	default:
		printStderr("Restoring to local filesystem (%v)...\n", restoreTargetPath)
		return restoreModeLocal
	}
}

func printRestoreStats(st restore.Stats) {
	printStderr("Restored %v files and %v directories (%v)\n", st.FileCount, st.DirCount, units.BytesStringBase10(st.TotalFileSize))
}

func runRestoreCommand(ctx context.Context, rep repo.Repository) error {
	oid, err := parseObjectID(ctx, rep, *restoreCommandSourcePath)
	if err != nil {
		return err
	}

	output, err := restoreOutput()
	if err != nil {
		return errors.Wrap(err, "unable to initialize output")
	}

	st, err := restore.Root(ctx, rep, output, oid)
	if err != nil {
		return err
	}

	printRestoreStats(st)

	return nil
}

func init() {
	addRestoreFlags(restoreCommand)
	restoreCommand.Action(repositoryAction(runRestoreCommand))
}
