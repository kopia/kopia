package ignorefs

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

const (
	maxSymlinkFollow = 30

	// backstop for loop detection, which needs local paths.
	maxFollowedDirDepth = 40
)

var errFollowLoop = errors.New("symlink points to a directory that is already being scanned")

// followChain holds the last directory scanned before each followed directory symlink on the way to a directory.
// Every other directory on that path is a prefix of one of these, which is all loop detection needs.
type followChain struct {
	parent *followChain
	tail   string // canonical path, empty if unknown
	depth  int
}

func (c *followChain) depthOrZero() int {
	if c == nil {
		return 0
	}

	return c.depth
}

type namedFile struct {
	fs.File

	name string
}

func (f *namedFile) Name() string {
	return f.name
}

type namedDirectory struct {
	fs.Directory

	name string
}

func (d *namedDirectory) Name() string {
	return d.name
}

func resolveEntry(ctx context.Context, entry fs.Symlink) (fs.Entry, error) {
	for range maxSymlinkFollow {
		target, err := entry.Resolve(ctx)
		if err != nil {
			link, _ := entry.Readlink(ctx)
			return nil, errors.Wrapf(err, "when resolving symlink %s of type %T, which points to %s", entry.Name(), entry, link)
		}

		next, ok := target.(fs.Symlink)
		if !ok {
			return target, nil
		}

		entry = next
	}

	return nil, errors.Wrapf(errTooManySymlinks, "cannot resolve '%q'", entry.Name())
}

// followSymlinkIfEnabled returns the file or directory sl points to under sl's name, or sl itself if it is not followed.
func (d *ignoreDirectory) followSymlinkIfEnabled(ctx context.Context, ic *ignoreContext, sl fs.Symlink) (fs.Entry, *followChain) {
	name := sl.Name()

	follow := ic.followSymlinks
	if pol := d.policyTree.Child(name).DefinedPolicy(); pol != nil {
		follow = pol.FilesPolicy.FollowSymlinks.OrDefault(follow)
	}

	if !follow {
		return sl, d.followed
	}

	target, err := resolveEntry(ctx, sl)
	if err != nil {
		log(ctx).Debugf("not following symlink %q: %v", name, err)
		return sl, d.followed
	}

	switch t := target.(type) {
	case fs.File:
		sl.Close()

		return &namedFile{t, name}, d.followed

	case fs.Directory:
		chain, err := d.chainForFollowedDirectory(t)
		if err != nil {
			log(ctx).Warnf("not following symlink %q: %v", name, err)
			t.Close()

			return sl, d.followed
		}

		sl.Close()

		return &namedDirectory{t, name}, chain

	default:
		log(ctx).Debugf("not following symlink %q: target is neither a file nor a directory (%T)", name, target)
		target.Close()

		return sl, d.followed
	}
}

func (d *ignoreDirectory) chainForFollowedDirectory(target fs.Directory) (*followChain, error) {
	depth := d.followed.depthOrZero()
	if depth >= maxFollowedDirDepth {
		return nil, errors.Errorf("more than %v directory symlinks followed in a row", maxFollowedDirDepth)
	}

	targetPath := target.LocalFilesystemPath()
	currentPath := canonicalPath(d.LocalFilesystemPath())

	if targetPath == "" || currentPath == "" {
		// not a local filesystem, only the depth limit applies.
		return &followChain{parent: d.followed, depth: depth + 1}, nil
	}

	sep := string(filepath.Separator)
	foldCase := runtime.GOOS == "windows"

	if isSameOrAncestorPath(targetPath, currentPath, sep, foldCase) {
		return nil, errors.Wrapf(errFollowLoop, "%q contains %q", targetPath, currentPath)
	}

	for c := d.followed; c != nil; c = c.parent {
		if c.tail != "" && isSameOrAncestorPath(targetPath, c.tail, sep, foldCase) {
			return nil, errors.Wrapf(errFollowLoop, "%q contains %q", targetPath, c.tail)
		}
	}

	return &followChain{parent: d.followed, tail: currentPath, depth: depth + 1}, nil
}

func canonicalPath(p string) string {
	if p == "" {
		return ""
	}

	resolved, err := filepath.EvalSymlinks(p)
	if err != nil {
		// fails on some paths that are valid, such as volume shadow copies.
		return p
	}

	return resolved
}

func isSameOrAncestorPath(ancestor, p, sep string, foldCase bool) bool {
	if foldCase {
		ancestor, p = strings.ToLower(ancestor), strings.ToLower(p)
	}

	ancestor = strings.TrimRight(ancestor, sep)
	p = strings.TrimRight(p, sep)

	return p == ancestor || strings.HasPrefix(p, ancestor+sep)
}
