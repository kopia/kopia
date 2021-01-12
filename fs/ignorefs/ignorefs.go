// Package ignorefs implements a wrapper that hides ignored files listed in '.kopiaignore' and in policies attached to directories.
package ignorefs

import (
	"bufio"
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/wcmatch"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/snapshot/policy"
)

var log = logging.GetContextLoggerFunc("ignorefs")

// IgnoreCallback is a function called by ignorefs to report whenever a file or directory is being ignored while listing its parent.
type IgnoreCallback func(path string, metadata fs.Entry)

type ignoreContext struct {
	parent *ignoreContext

	onIgnore []IgnoreCallback

	dotIgnoreFiles []string                  // which files to look for more ignore rules
	matchers       []wcmatch.WildcardMatcher // current set of rules to ignore files
	maxFileSize    int64                     // maximum size of file allowed

	oneFileSystem bool // should we enter other mounted filesystems
}

func (c *ignoreContext) shouldIncludeByName(path string, e fs.Entry) bool {
	shouldIgnore := false

	// Start by checking with any ignores defined in a parent directory (if there is one).
	// Any matches here may be negated by .ignore-files in lower directories.
	if c.parent != nil {
		shouldIgnore = !c.parent.shouldIncludeByName(path, e)
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
			oi(path, e)
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

func isCorrectCacheDirSignature(ctx context.Context, f fs.File) (bool, error) {
	const (
		validSignature    = repo.CacheDirMarkerHeader
		validSignatureLen = len(validSignature)
	)

	if f.Size() < int64(validSignatureLen) {
		return false, nil
	}

	r, err := f.Open(ctx)
	if err != nil {
		return false, errors.Wrap(err, "unable to open cache dir marker file")
	}

	defer r.Close() //nolint:errcheck

	sig := make([]byte, validSignatureLen)

	if _, err := r.Read(sig); err != nil {
		return false, errors.Wrap(err, "unable to read cache dir marker file")
	}

	return string(sig) == validSignature, nil
}

func (d *ignoreDirectory) skipCacheDirectory(ctx context.Context, entries fs.Entries, relativePath string, policyTree *policy.Tree) fs.Entries {
	if !policyTree.EffectivePolicy().FilesPolicy.IgnoreCacheDirectoriesOrDefault(true) {
		return entries
	}

	f, ok := entries.FindByName(repo.CacheDirMarkerFile).(fs.File)
	if ok {
		correct, err := isCorrectCacheDirSignature(ctx, f)
		if err != nil {
			log(ctx).Debugf("unable to check cache dir signature, assuming not a cache directory: %v", err)
			return entries
		}

		if correct {
			// if the given directory contains a marker file used for kopia cache, pretend the directory was empty.
			for _, oi := range d.parentContext.onIgnore {
				oi(relativePath, d)
			}

			return nil
		}
	}

	return entries
}

func (d *ignoreDirectory) Readdir(ctx context.Context) (fs.Entries, error) {
	entries, err := d.Directory.Readdir(ctx)
	if err != nil {
		// nolint:wrapcheck
		return nil, err
	}

	entries = d.skipCacheDirectory(ctx, entries, d.relativePath, d.policyTree)

	thisContext, err := d.buildContext(ctx, entries)
	if err != nil {
		return nil, err
	}

	result := make(fs.Entries, 0, len(entries))

	for _, e := range entries {
		if !thisContext.shouldIncludeByName(d.relativePath+"/"+e.Name(), e) {
			continue
		}

		if maxSize := thisContext.maxFileSize; maxSize > 0 && e.Size() > maxSize {
			continue
		}

		if !thisContext.shouldIncludeByDevice(e, d) {
			continue
		}

		if dir, ok := e.(fs.Directory); ok {
			e = &ignoreDirectory{d.relativePath + "/" + e.Name(), thisContext, d.policyTree.Child(e.Name()), dir}
		}

		result = append(result, e)
	}

	return result, nil
}

func (d *ignoreDirectory) buildContext(ctx context.Context, entries fs.Entries) (*ignoreContext, error) {
	effectiveDotIgnoreFiles := d.parentContext.dotIgnoreFiles

	pol := d.policyTree.DefinedPolicy()
	if pol != nil {
		effectiveDotIgnoreFiles = pol.FilesPolicy.DotIgnoreFiles
	}

	var foundDotIgnoreFiles bool

	for _, dotfile := range effectiveDotIgnoreFiles {
		if e := entries.FindByName(dotfile); e != nil {
			foundDotIgnoreFiles = true
		}
	}

	if !foundDotIgnoreFiles && pol == nil {
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

	if err := newic.loadDotIgnoreFiles(ctx, d.relativePath, entries, effectiveDotIgnoreFiles); err != nil {
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

	c.oneFileSystem = fp.OneFileSystemOrDefault(false)

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

func (c *ignoreContext) loadDotIgnoreFiles(ctx context.Context, dirPath string, entries fs.Entries, dotIgnoreFiles []string) error {
	for _, dotIgnoreFile := range dotIgnoreFiles {
		e := entries.FindByName(dotIgnoreFile)
		if e == nil {
			// no dotfile
			continue
		}

		f, ok := e.(fs.File)
		if !ok {
			// not a file
			continue
		}

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
