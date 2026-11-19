package snapshotfs

import (
	"container/list"
	"context"
	"os"
	"path"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
)

const nameSearchCacheLimit = 4096

// NameMatch describes an archived entry. Path is relative to the searched root;
// the root itself is represented by ".". File contents are never opened.
type NameMatch struct {
	Path     string
	ObjectID object.ID
	Mode     os.FileMode
	Size     int64
}

type nameSearchCacheEntry struct {
	id      object.ID
	matches []NameMatch
}

// NameSearcher finds literal substrings in entry names. Reuse one searcher for
// sequential searches to reuse matching subtrees by their content-addressed IDs.
// Its bounded cache stores metadata, not open entries or file contents.
type NameSearcher struct {
	pattern    string
	ignoreCase bool
	cache      map[object.ID]*list.Element
	lru        *list.List
	cacheSize  int
}

// NewNameSearcher creates a literal name search. An empty pattern is rejected.
func NewNameSearcher(pattern string, ignoreCase bool) (*NameSearcher, error) {
	if pattern == "" {
		return nil, errors.New("search pattern must not be empty")
	}

	if ignoreCase {
		pattern = strings.ToLower(pattern)
	}

	return &NameSearcher{
		pattern: pattern, ignoreCase: ignoreCase,
		cache: map[object.ID]*list.Element{}, lru: list.New(),
	}, nil
}

// Search visits matching files, directories and symlinks without following links.
// It stops on cancellation, a directory read error or an error returned by visit.
// The caller retains ownership of root; child entries and iterators are closed.
func (s *NameSearcher) Search(ctx context.Context, root fs.Entry, visit func(NameMatch) error) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck
	}

	if bad, ok := root.(fs.ErrorEntry); ok {
		return errors.Wrapf(bad.ErrorInfo(), "reading search root %q", root.Name())
	}

	if s.matches(root.Name()) {
		if err := visit(nameMatch(root, ".")); err != nil {
			return err
		}
	}

	if dir, ok := root.(fs.Directory); ok {
		_, _, err := s.searchDirectory(ctx, dir, ".", visit)
		return err
	}

	return nil
}

func (s *NameSearcher) matches(name string) bool {
	if s.ignoreCase {
		name = strings.ToLower(name)
	}

	return strings.Contains(name, s.pattern)
}

func nameMatch(entry fs.Entry, relative string) NameMatch {
	m := NameMatch{Path: relative, Mode: entry.Mode(), Size: entry.Size()}
	if hasID, ok := entry.(object.HasObjectID); ok {
		m.ObjectID = hasID.ObjectID()
	}

	return m
}

func appendNameMatch(matches []NameMatch, cacheable bool, match NameMatch) ([]NameMatch, bool) {
	if !cacheable || len(matches)+1 >= nameSearchCacheLimit {
		return nil, false
	}

	return append(matches, match), true
}

func (s *NameSearcher) remember(id object.ID, matches []NameMatch) {
	if id == object.EmptyID {
		return
	}

	for s.cacheSize+len(matches)+1 > nameSearchCacheLimit {
		oldest := s.lru.Back()
		cached := oldest.Value.(nameSearchCacheEntry) //nolint:forcetypeassert
		s.cacheSize -= len(cached.matches) + 1
		delete(s.cache, cached.id)
		s.lru.Remove(oldest)
	}

	s.cache[id] = s.lru.PushFront(nameSearchCacheEntry{id, matches})
	s.cacheSize += len(matches) + 1
}

func (s *NameSearcher) searchDirectory(ctx context.Context, dir fs.Directory, prefix string, visit func(NameMatch) error) ([]NameMatch, bool, error) {
	id := nameMatch(dir, "").ObjectID
	if cached, ok := s.cache[id]; ok {
		s.lru.MoveToFront(cached)

		matches := cached.Value.(nameSearchCacheEntry).matches //nolint:forcetypeassert
		for _, match := range matches {
			if err := ctx.Err(); err != nil {
				return nil, false, err //nolint:wrapcheck
			}

			match.Path = path.Join(prefix, match.Path)
			if err := visit(match); err != nil {
				return nil, false, err
			}
		}

		return matches, true, nil
	}

	iter, err := dir.Iterate(ctx)
	if err != nil {
		return nil, false, errors.Wrapf(err, "reading directory %q", prefix)
	}
	defer iter.Close()

	var matches []NameMatch

	cacheable := true
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err //nolint:wrapcheck
		}

		entry, err := iter.Next(ctx)
		if err != nil {
			return nil, false, errors.Wrapf(err, "reading directory %q", prefix)
		}

		if entry == nil {
			break
		}

		found, complete, err := s.searchChild(ctx, entry, prefix, visit)
		if err != nil {
			return nil, false, err
		}

		if !complete {
			matches, cacheable = nil, false
		}

		for _, match := range found {
			matches, cacheable = appendNameMatch(matches, cacheable, match)
		}
	}

	if cacheable {
		s.remember(id, matches)
	}

	return matches, cacheable, nil
}

func (s *NameSearcher) searchChild(ctx context.Context, entry fs.Entry, prefix string, visit func(NameMatch) error) ([]NameMatch, bool, error) {
	defer entry.Close()

	if bad, ok := entry.(fs.ErrorEntry); ok {
		return nil, false, errors.Wrapf(bad.ErrorInfo(), "reading entry %q", path.Join(prefix, entry.Name()))
	}

	var matches []NameMatch
	if s.matches(entry.Name()) {
		match := nameMatch(entry, path.Join(prefix, entry.Name()))
		if err := visit(match); err != nil {
			return nil, false, err
		}

		match.Path = entry.Name()
		matches = append(matches, match)
	}

	if dir, ok := entry.(fs.Directory); ok {
		children, cacheable, err := s.searchDirectory(ctx, dir, path.Join(prefix, entry.Name()), visit)
		if err != nil || !cacheable {
			return nil, false, err
		}

		for _, child := range children {
			child.Path = path.Join(entry.Name(), child.Path)
			matches, cacheable = appendNameMatch(matches, cacheable, child)
		}

		return matches, cacheable, nil
	}

	return matches, true, nil
}
