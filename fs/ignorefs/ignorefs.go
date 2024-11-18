// Package ignorefs implements a wrapper that hides ignored files listed in '.kopiaignore' and in policies attached to directories.
package ignorefs

import (
	"bufio"
	"context"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/cachedir"
	"github.com/kopia/kopia/internal/wcmatch"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

var (
	log                = logging.Module("ignorefs")
	errSymlinkNotAFile = errors.New("Symlink does not link to a file")
)

// IgnoreCallback is a function called by ignorefs to report whenever a file or directory is being ignored while listing its parent.
type IgnoreCallback func(ctx context.Context, path string, metadata fs.Entry, pol *policy.Tree)

type ignoreContext struct {
	parent *ignoreContext

	onIgnore []IgnoreCallback

	dotIgnoreFiles []string                  // which files to look for more ignore rules
	matchers       []wcmatch.WildcardMatcher // current set of rules to ignore files
	maxFileSize    int64                     // maximum size of file allowed

	oneFileSystem bool // should we enter other mounted filesystems
}

func (c *ignoreContext) shouldIncludeByName(ctx context.Context, path string, e fs.Entry, policyTree *policy.Tree) bool {
	shouldIgnore := false

	// Start by checking with any ignores defined in a parent directory (if there is one).
	// Any matches here may be negated by .ignore-files in lower directories.
	if c.parent != nil {
		shouldIgnore = !c.parent.shouldIncludeByName(ctx, path, e, policyTree)
	}

	for _, m := range c.matchers {
		// If we already matched a pattern and concluded that the path should be ignored, we only check
		// negated patterns (and vice versa)
		if !shouldIgnore && !m.Negated() || shouldIgnore && m.Negated() {
			shouldIgnore = m.Match(trimLeadingCurrentDir(path), e.IsDir())
		}
	}

	if shouldIgnore {
		for _, oi := range c.onIgnore {
			oi(ctx, strings.TrimPrefix(path, "./"), e, policyTree)
		}

		return false
	}

	return true
}

func (c *ignoreContext) shouldIncludeByDevice(e fs.Entry, parent *ignoreDirectory) bool {
	if !c.oneFileSystem {
		return true
	}

	return e.Device().Dev == parent.Device().Dev
}

type ignoreDirectory struct {
	relativePath  string
	parentContext *ignoreContext
	policyTree    *policy.Tree

	fs.Directory
}

func isCorrectCacheDirSignature(ctx context.Context, f fs.File) error {
	const (
		validSignature    = cachedir.CacheDirMarkerHeader
		validSignatureLen = len(validSignature)
	)

	if f.Size() < int64(validSignatureLen) {
		return errors.New("cache dir marker file too short")
	}

	r, err := f.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to open cache dir marker file")
	}

	defer r.Close() //nolint:errcheck

	sig := make([]byte, validSignatureLen)

	if _, err := r.Read(sig); err != nil {
		return errors.Wrap(err, "unable to read cache dir marker file")
	}

	if string(sig) != validSignature {
		return errors.New("invalid cache dir marker file signature")
	}

	return nil
}

func (d *ignoreDirectory) skipCacheDirectory(ctx context.Context, relativePath string, policyTree *policy.Tree) bool {
	if !policyTree.EffectivePolicy().FilesPolicy.IgnoreCacheDirectories.OrDefault(true) {
		return false
	}

	e, err := d.Directory.Child(ctx, cachedir.CacheDirMarkerFile)
	if err != nil {
		return false
	}

	f, ok := e.(fs.File)
	if !ok {
		return false
	}

	if err := isCorrectCacheDirSignature(ctx, f); err != nil {
		log(ctx).Debugf("unable to check cache dir signature, assuming not a cache directory: %v", err)
		return false
	}

	// if the given directory contains a marker file used for kopia cache, pretend the directory was empty.
	for _, oi := range d.parentContext.onIgnore {
		oi(ctx, strings.TrimPrefix(relativePath, "./"), d, policyTree)
	}

	return true
}

// Make sure that ignoreDirectory implements HasDirEntryFromPlaceholder.
var _ snapshot.HasDirEntryOrNil = (*ignoreDirectory)(nil)

func (d *ignoreDirectory) DirEntryOrNil(ctx context.Context) (*snapshot.DirEntry, error) {
	if defp, ok := d.Directory.(snapshot.HasDirEntryOrNil); ok {
		//nolint:wrapcheck
		return defp.DirEntryOrNil(ctx)
	}
	// Ignored directories do not have DirEntry objects.
	return nil, nil
}

type ignoreDirIterator struct {
	//nolint:containedctx
	ctx         context.Context
	d           *ignoreDirectory
	inner       fs.DirectoryIterator
	thisContext *ignoreContext
}

func (i *ignoreDirIterator) Next(ctx context.Context) (fs.Entry, error) {
	cur, err := i.inner.Next(ctx)

	for cur != nil {
		//nolint:contextcheck
		if wrapped, ok := i.d.maybeWrappedChildEntry(i.ctx, i.thisContext, cur); ok {
			return wrapped, nil
		}

		cur, err = i.inner.Next(ctx)
	}

	return nil, err //nolint:wrapcheck
}

func (i *ignoreDirIterator) Close() {
	i.inner.Close()

	*i = ignoreDirIterator{}
	ignoreDirIteratorPool.Put(i)
}

func (d *ignoreDirectory) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	if d.skipCacheDirectory(ctx, d.relativePath, d.policyTree) {
		return fs.StaticIterator(nil, nil), nil
	}

	thisContext, err := d.buildContext(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "in ignoreDirectory.Iterate, when building context")
	}

	inner, err := d.Directory.Iterate(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "in ignoreDirectory.Iterate, when creating iterator")
	}

	it := ignoreDirIteratorPool.Get().(*ignoreDirIterator) //nolint:forcetypeassert
	it.ctx = ctx
	it.d = d
	it.inner = inner
	it.thisContext = thisContext

	return it, nil
}

//nolint:gochecknoglobals
var ignoreDirectoryPool = sync.Pool{
	New: func() any { return &ignoreDirectory{} },
}

//nolint:gochecknoglobals
var ignoreDirIteratorPool = sync.Pool{
	New: func() any { return &ignoreDirIterator{} },
}

func (d *ignoreDirectory) Close() {
	d.Directory.Close()

	*d = ignoreDirectory{}
	ignoreDirectoryPool.Put(d)
}

func (d *ignoreDirectory) maybeWrappedChildEntry(ctx context.Context, ic *ignoreContext, e fs.Entry) (fs.Entry, bool) {
	s := d.relativePath + "/" + e.Name()

	if !ic.shouldIncludeByName(ctx, s, e, d.policyTree) {
		return nil, false
	}

	if maxSize := ic.maxFileSize; maxSize > 0 && e.Size() > maxSize {
		return nil, false
	}

	if !ic.shouldIncludeByDevice(e, d) {
		return nil, false
	}

	if dir, ok := e.(fs.Directory); ok {
		id := ignoreDirectoryPool.Get().(*ignoreDirectory) //nolint:forcetypeassert

		id.relativePath = s
		id.parentContext = ic
		id.policyTree = d.policyTree.Child(e.Name())
		id.Directory = dir

		return id, true
	}

	return e, true
}

func (d *ignoreDirectory) Child(ctx context.Context, name string) (fs.Entry, error) {
	if d.skipCacheDirectory(ctx, d.relativePath, d.policyTree) {
		return nil, fs.ErrEntryNotFound
	}

	e, err := d.Directory.Child(ctx, name)
	if err != nil {
		//nolint:wrapcheck
		return nil, err
	}

	thisContext, err := d.buildContext(ctx)
	if err != nil {
		return nil, err
	}

	if wrapped, ok := d.maybeWrappedChildEntry(ctx, thisContext, e); ok {
		return wrapped, nil
	}

	return nil, fs.ErrEntryNotFound
}

func resolveSymlink(ctx context.Context, entry fs.Symlink) (fs.File, error) {
	for {
		target, err := entry.Resolve(ctx)
		if err != nil {
			link, _ := entry.Readlink(ctx)
			return nil, errors.Wrapf(err, "when resolving symlink %s of type %T, which points to %s", entry.Name(), entry, link)
		}

		switch t := target.(type) {
		case fs.File:
			return t, nil
		case fs.Symlink:
			entry = t
			continue
		default:
			return nil, errors.Wrapf(errSymlinkNotAFile, "%s does not eventually link to a file", entry.Name())
		}
	}
}

func (d *ignoreDirectory) buildContext(ctx context.Context) (*ignoreContext, error) {
	effectiveDotIgnoreFiles := d.parentContext.dotIgnoreFiles

	pol := d.policyTree.DefinedPolicy()
	if pol != nil {
		effectiveDotIgnoreFiles = pol.FilesPolicy.DotIgnoreFiles
	}

	var dotIgnoreFiles []fs.File

	for _, dotfile := range effectiveDotIgnoreFiles {
		if e, err := d.Directory.Child(ctx, dotfile); err == nil {
			switch entry := e.(type) {
			case fs.File:
				dotIgnoreFiles = append(dotIgnoreFiles, entry)

			case fs.Symlink:
				target, err := resolveSymlink(ctx, entry)
				if err != nil {
					return nil, err
				}

				dotIgnoreFiles = append(dotIgnoreFiles, target)
			}
		}
	}

	if len(dotIgnoreFiles) == 0 && pol == nil {
		// no dotfiles and no policy at this level, reuse parent ignore rules
		return d.parentContext, nil
	}

	newic := &ignoreContext{
		parent:         d.parentContext,
		onIgnore:       d.parentContext.onIgnore,
		dotIgnoreFiles: effectiveDotIgnoreFiles,
		maxFileSize:    d.parentContext.maxFileSize,
		oneFileSystem:  d.parentContext.oneFileSystem,
	}

	if pol != nil {
		if err := newic.overrideFromPolicy(&pol.FilesPolicy, d.relativePath); err != nil {
			return nil, err
		}
	}

	if err := newic.loadDotIgnoreFiles(ctx, d.relativePath, dotIgnoreFiles); err != nil {
		return nil, err
	}

	return newic, nil
}

func (c *ignoreContext) overrideFromPolicy(fp *policy.FilesPolicy, dirPath string) error {
	if fp.NoParentDotIgnoreFiles {
		c.dotIgnoreFiles = nil
	}

	if fp.NoParentIgnoreRules {
		c.matchers = nil
	}

	c.dotIgnoreFiles = combineAndDedupe(c.dotIgnoreFiles, fp.DotIgnoreFiles)
	if fp.MaxFileSize != 0 {
		c.maxFileSize = fp.MaxFileSize
	}

	c.oneFileSystem = fp.OneFileSystem.OrDefault(false)

	// append policy-level rules
	for _, rule := range fp.IgnoreRules {
		m, err := wcmatch.NewWildcardMatcher(rule, wcmatch.IgnoreCase(false), wcmatch.BaseDir(trimLeadingCurrentDir(dirPath)))
		if err != nil {
			return errors.Wrapf(err, "unable to parse ignore entry %v", dirPath)
		}

		c.matchers = append(c.matchers, *m)
	}

	return nil
}

func (c *ignoreContext) loadDotIgnoreFiles(ctx context.Context, dirPath string, dotIgnoreFiles []fs.File) error {
	for _, f := range dotIgnoreFiles {
		matchers, err := parseIgnoreFile(ctx, dirPath, f)
		if err != nil {
			return errors.Wrapf(err, "unable to parse ignore file %v", f.Name())
		}

		c.matchers = append(c.matchers, matchers...)
	}

	return nil
}

func combineAndDedupe(slices ...[]string) []string {
	var result []string

	existing := map[string]bool{}

	for _, slice := range slices {
		for _, it := range slice {
			if existing[it] {
				continue
			}

			existing[it] = true

			result = append(result, it)
		}
	}

	return result
}

func parseIgnoreFile(ctx context.Context, baseDir string, file fs.File) ([]wcmatch.WildcardMatcher, error) {
	f, err := file.Open(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open ignore file")
	}
	defer f.Close() //nolint:errcheck

	var matchers []wcmatch.WildcardMatcher

	// Remove the "current directory" indicator from the baseDir if present, since wcmatch does
	// not deal with that.
	if strings.HasPrefix(baseDir, "./") || baseDir == "." {
		baseDir = baseDir[1:]
	}

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()

		if strings.HasPrefix(line, "#") {
			// ignore comments
			continue
		}

		if strings.TrimSpace(line) == "" {
			// ignore empty lines
			continue
		}

		m, err := wcmatch.NewWildcardMatcher(line, wcmatch.IgnoreCase(false), wcmatch.BaseDir(trimLeadingCurrentDir(baseDir)))
		if err != nil {
			return nil, errors.Wrapf(err, "unable to parse ignore entry %v", line)
		}

		matchers = append(matchers, *m)
	}

	return matchers, nil
}

// trimLeadingCurrentDir strips a leading "./" from a directory, or replace with empty string if the directory contains only a ".".
func trimLeadingCurrentDir(dir string) string {
	if dir == "." || strings.HasPrefix(dir, "./") {
		dir = dir[1:]
	}

	return dir
}

// Option modifies the behavior of ignorefs.
type Option func(parentContext *ignoreContext)

// New returns a fs.Directory that wraps another fs.Directory and hides files specified in the ignore dotfiles.
func New(dir fs.Directory, policyTree *policy.Tree, options ...Option) fs.Directory {
	rootContext := &ignoreContext{}

	for _, opt := range options {
		opt(rootContext)
	}

	return &ignoreDirectory{".", rootContext, policyTree, dir}
}

var _ fs.Directory = &ignoreDirectory{}

// ReportIgnoredFiles returns an Option causing ignorefs to call the provided function whenever a file or directory is ignored.
func ReportIgnoredFiles(f IgnoreCallback) Option {
	return func(ic *ignoreContext) {
		if f != nil {
			ic.onIgnore = append(ic.onIgnore, f)
		}
	}
}
