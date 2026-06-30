package ignorefs

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot/policy"
)

// Checker evaluates whether paths would be excluded by current ignore rules
// (.kopiaignore files and policy IgnoreRules), using the same name-matching
// logic as ignorefs during snapshot. It does not apply MaxFileSize,
// OneFileSystem, or cache-directory filters.
type Checker struct {
	root       fs.Directory
	policyTree *policy.Tree
	rootCtx    *ignoreContext

	mu       sync.Mutex
	dirCache map[string]*checkerDirContext
}

type checkerDirContext struct {
	ctx      *ignoreContext
	polTree  *policy.Tree
	fromDisk bool
}

// NewChecker returns a Checker that reads .kopiaignore files from root on demand.
func NewChecker(root fs.Directory, policyTree *policy.Tree) *Checker {
	return &Checker{
		root:       root,
		policyTree: policyTree,
		rootCtx:    &ignoreContext{},
		dirCache:   map[string]*checkerDirContext{},
	}
}

// IsIgnored reports whether relPath would be excluded by current ignore rules.
// relPath uses snapshot-style paths (e.g. "dir1/file.txt" without a leading "./").
func (c *Checker) IsIgnored(ctx context.Context, relPath string, isDir bool) (bool, error) {
	ignorePath := normalizeIgnorePath(relPath)

	components := strings.Split(strings.TrimPrefix(ignorePath, "./"), "/")
	if components[0] == "" {
		components = nil
	}

	for i := 0; i < len(components)-1; i++ {
		subPath := "./" + strings.Join(components[:i+1], "/")

		parentPath, name := splitParentIgnorePath(subPath)

		ignored, err := c.isIgnoredAtParent(ctx, parentPath, name, subPath, true)
		if err != nil {
			return false, err
		}

		if ignored {
			return true, nil
		}
	}

	parentPath, name := splitParentIgnorePath(ignorePath)
	if name == "" {
		return false, nil
	}

	return c.isIgnoredAtParent(ctx, parentPath, name, ignorePath, isDir)
}

func (c *Checker) isIgnoredAtParent(ctx context.Context, parentPath, name, ignorePath string, isDir bool) (bool, error) {
	ic, polTree, err := c.contextForDir(ctx, parentPath)
	if err != nil {
		return false, err
	}

	stub := ignoreEntryStub{name: name, isDir: isDir}

	return !ic.shouldIncludeByName(ctx, ignorePath, stub, polTree), nil
}

func (c *Checker) contextForDir(ctx context.Context, relativePath string) (*ignoreContext, *policy.Tree, error) {
	c.mu.Lock()
	if cached, ok := c.dirCache[relativePath]; ok {
		c.mu.Unlock()

		return cached.ctx, cached.polTree, nil
	}
	c.mu.Unlock()

	ic, polTree, err := c.buildContextForDir(ctx, relativePath)

	c.mu.Lock()
	c.dirCache[relativePath] = &checkerDirContext{ctx: ic, polTree: polTree, fromDisk: err == nil}
	c.mu.Unlock()

	return ic, polTree, err
}

func (c *Checker) buildContextForDir(ctx context.Context, relativePath string) (*ignoreContext, *policy.Tree, error) {
	if relativePath == "." {
		ic, err := buildContextForDirectory(ctx, c.root, ".", c.rootCtx, c.policyTree)
		if err != nil {
			return nil, nil, err
		}

		return ic, c.policyTree, nil
	}

	parentPath, name := splitParentIgnorePath(relativePath)

	parentCtx, parentPolTree, err := c.contextForDir(ctx, parentPath)
	if err != nil {
		return nil, nil, err
	}

	parentDir, err := c.dirAt(ctx, parentPath)
	if err != nil {
		return parentCtx, parentPolTree, nil
	}

	child, err := parentDir.Child(ctx, name)
	if err != nil {
		return parentCtx, parentPolTree, nil
	}

	subDir, ok := child.(fs.Directory)
	if !ok {
		return parentCtx, parentPolTree, nil
	}

	childPolTree := parentPolTree.Child(name)

	ic, err := buildContextForDirectory(ctx, subDir, relativePath, parentCtx, childPolTree)
	if err != nil {
		return nil, nil, err
	}

	return ic, childPolTree, nil
}

func (c *Checker) dirAt(ctx context.Context, relativePath string) (fs.Directory, error) {
	if relativePath == "." {
		return c.root, nil
	}

	dir := c.root

	for _, p := range strings.Split(strings.TrimPrefix(relativePath, "./"), "/") {
		e, err := dir.Child(ctx, p)
		if err != nil {
			return nil, err //nolint:wrapcheck
		}

		sub, ok := e.(fs.Directory)
		if !ok {
			return nil, fs.ErrEntryNotFound
		}

		dir = sub
	}

	return dir, nil
}

func normalizeIgnorePath(relPath string) string {
	relPath = strings.TrimPrefix(relPath, "/")
	if relPath == "" || relPath == "." {
		return "."
	}

	return "./" + relPath
}

func splitParentIgnorePath(p string) (parent, name string) {
	if p == "." {
		return ".", ""
	}

	idx := strings.LastIndex(p, "/")
	if idx < 0 {
		return ".", strings.TrimPrefix(p, "./")
	}

	if idx <= 1 {
		// "./name" -> parent is root "."
		return ".", p[idx+1:]
	}

	return p[:idx], p[idx+1:]
}

type ignoreEntryStub struct {
	name  string
	isDir bool
}

func (e ignoreEntryStub) Name() string               { return e.name }
func (e ignoreEntryStub) IsDir() bool                { return e.isDir }
func (e ignoreEntryStub) Mode() os.FileMode              { return 0 }
func (e ignoreEntryStub) Size() int64                    { return 0 }
func (e ignoreEntryStub) ModTime() time.Time             { return time.Time{} }
func (e ignoreEntryStub) Sys() any                     { return nil }
func (e ignoreEntryStub) Owner() fs.OwnerInfo        { return fs.OwnerInfo{} }
func (e ignoreEntryStub) Device() fs.DeviceInfo      { return fs.DeviceInfo{} }
func (e ignoreEntryStub) LocalFilesystemPath() string { return "" }
func (e ignoreEntryStub) Close()                     {}
