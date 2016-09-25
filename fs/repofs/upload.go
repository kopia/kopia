package repofs

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
)

const (
	maxBundleFileSize = 65536
)

func hashEntryMetadata(w io.Writer, e *fs.EntryMetadata) {
	binary.Write(w, binary.LittleEndian, e.Name)
	binary.Write(w, binary.LittleEndian, e.ModTime.UnixNano())
	binary.Write(w, binary.LittleEndian, e.FileMode())
	binary.Write(w, binary.LittleEndian, e.FileSize)
	binary.Write(w, binary.LittleEndian, e.UserID)
	binary.Write(w, binary.LittleEndian, e.GroupID)
}

func metadataHash(e *fs.EntryMetadata) uint64 {
	h := fnv.New64a()
	hashEntryMetadata(h, e)
	return h.Sum64()
}

func bundleHash(b *bundle) uint64 {
	h := fnv.New64a()
	hashEntryMetadata(h, b.metadata)
	for i, f := range b.files {
		binary.Write(h, binary.LittleEndian, i)
		hashEntryMetadata(h, f.Metadata())
	}
	return h.Sum64()
}

// uploadResult stores results of an upload.
type uploadResult struct {
	ObjectID   repo.ObjectID
	ManifestID repo.ObjectID
	Cancelled  bool

	Stats struct {
		CachedDirectories    int
		CachedFiles          int
		NonCachedDirectories int
		NonCachedFiles       int
	}
}

// uploader supports efficient uploading files and directories to repository.
type uploader struct {
	repo *repo.Repository
}

func (u *uploader) uploadFileInternal(f fs.File, relativePath string, forceStored bool) (*dirEntry, uint64, error) {
	log.Printf("Uploading file %v", relativePath)
	t0 := time.Now()
	file, err := f.Open()
	if err != nil {
		return nil, 0, fmt.Errorf("unable to open file: %v", err)
	}
	defer file.Close()

	writer := u.repo.NewWriter(
		repo.WithDescription("FILE:" + f.Metadata().Name),
	)
	defer writer.Close()

	written, err := io.Copy(writer, file)
	if err != nil {
		return nil, 0, err
	}

	e2, err := file.EntryMetadata()
	if err != nil {
		return nil, 0, err
	}

	r, err := writer.Result(forceStored)
	if err != nil {
		return nil, 0, err
	}

	de := newDirEntry(e2, r)
	de.FileSize = written
	dt := time.Since(t0)
	log.Printf("Uploaded file %v, %v bytes in %v. %v", relativePath, written, dt, bytesPerSecond(written, dt))

	return de, metadataHash(&de.EntryMetadata), nil
}

func bytesPerSecond(bytes int64, duration time.Duration) string {
	if duration == 0 {
		return "0 B/s"
	}

	bps := float64(bytes) / duration.Seconds()

	if bps >= 700000 {
		return fmt.Sprintf("%.2f MB/s", bps/1000000)
	}

	if bps >= 700 {
		return fmt.Sprintf("%.2f KB/s", bps/1000)
	}

	return fmt.Sprintf("%.2f B/s", bps)
}

func newDirEntry(md *fs.EntryMetadata, oid repo.ObjectID) *dirEntry {
	return &dirEntry{
		EntryMetadata: *md,
		ObjectID:      oid,
	}
}

func (u *uploader) uploadBundleInternal(b *bundle) (*dirEntry, uint64, error) {
	bundleMetadata := b.Metadata()

	log.Printf("uploading bundle %v (%v files)", bundleMetadata.Name, len(b.files))
	t0 := time.Now()

	writer := u.repo.NewWriter(
		repo.WithDescription("BUNDLE:" + bundleMetadata.Name),
	)
	defer writer.Close()

	var uploadedFiles []fs.File
	var err error

	de := newDirEntry(bundleMetadata, repo.NullObjectID)
	var totalBytes int64

	for _, fileEntry := range b.files {
		file, err := fileEntry.Open()
		if err != nil {
			return nil, 0, err
		}

		fileMetadata, err := file.EntryMetadata()
		if err != nil {
			return nil, 0, err
		}

		written, err := io.Copy(writer, file)
		if err != nil {
			return nil, 0, err
		}

		fileMetadata.FileSize = written
		de.BundledChildren = append(de.BundledChildren, newDirEntry(fileMetadata, repo.NullObjectID))

		uploadedFiles = append(uploadedFiles, &bundledFile{metadata: fileMetadata})
		totalBytes += written
		file.Close()
	}

	b.files = uploadedFiles
	de.ObjectID, err = writer.Result(true)
	dt := time.Since(t0)
	log.Printf("Uploaded bundle %v (%v files) %v bytes in %v. %v", bundleMetadata.Name, len(b.files), totalBytes, dt, bytesPerSecond(totalBytes, dt))
	if err != nil {
		return nil, 0, err
	}

	return de, bundleHash(b), nil
}

// Upload uploads contents of the specified filesystem entry (file or directory) to the repository and updates given manifest with statistics.
// Old snapshot manifest, when provided can be used to speed up backups by utilizing hash cache.
func Upload(repository *repo.Repository, source fs.Entry, sourceInfo *SnapshotSourceInfo, old *Snapshot) (*Snapshot, error) {
	u := &uploader{
		repo: repository,
	}

	s := &Snapshot{
		Source:    *sourceInfo,
		StartTime: time.Now(),
	}

	var hashCacheID *repo.ObjectID

	if old != nil {
		hashCacheID = &old.HashCacheID
	}

	var r *uploadResult
	var err error
	switch entry := source.(type) {
	case fs.Directory:
		r, err = u.uploadDir(entry, hashCacheID)
	case fs.File:
		r, err = u.uploadFile(entry)
	default:
		return nil, fmt.Errorf("unsupported source: %v", s.Source)
	}
	s.EndTime = time.Now()
	if err != nil {
		return nil, err
	}
	s.RootObjectID = r.ObjectID
	s.HashCacheID = r.ManifestID
	stats := u.repo.Stats
	s.Stats = &stats

	return s, nil
}

// uploadFile uploads the specified File to the repository.
func (u *uploader) uploadFile(file fs.File) (*uploadResult, error) {
	result := &uploadResult{}
	e, _, err := u.uploadFileInternal(file, file.Metadata().Name, true)
	result.ObjectID = e.ObjectID
	return result, err
}

// uploadDir uploads the specified Directory to the repository.
// An optional ID of a hash-cache object may be provided, in which case the uploader will use its
// contents to avoid hashing
func (u *uploader) uploadDir(dir fs.Directory, hashCacheID *repo.ObjectID) (*uploadResult, error) {
	var hcr hashcacheReader
	var err error

	if hashCacheID != nil {
		if r, err := u.repo.Open(*hashCacheID); err == nil {
			hcr.open(r)
		}
	}

	mw := u.repo.NewWriter(
		repo.WithDescription("HASHCACHE:"+dir.Metadata().Name),
		repo.WithBlockNamePrefix("H"),
	)
	defer mw.Close()
	hcw := newHashCacheWriter(mw)

	result := &uploadResult{}
	result.ObjectID, _, _, err = u.uploadDirInternal(result, dir, ".", hcw, &hcr, true)
	if err != nil {
		return result, err
	}

	hcw.Finalize()

	result.ManifestID, err = mw.Result(true)
	return result, err
}

func (u *uploader) uploadDirInternal(
	result *uploadResult,
	dir fs.Directory,
	relativePath string,
	hcw *hashcacheWriter,
	hcr *hashcacheReader,
	forceStored bool,
) (repo.ObjectID, uint64, bool, error) {
	log.Printf("Uploading dir %v", relativePath)
	defer log.Printf("Finished uploading dir %v", relativePath)

	entries, err := dir.Readdir()
	if err != nil {
		return repo.NullObjectID, 0, false, err
	}

	entries = u.bundleEntries(entries)

	writer := u.repo.NewWriter(
		repo.WithDescription("DIR:" + relativePath),
	)

	dw := newDirWriter(writer)
	defer writer.Close()

	allCached := true

	dirHasher := fnv.New64a()
	dirHasher.Write([]byte(relativePath))
	dirHasher.Write([]byte{0})

	for _, entry := range entries {
		e := entry.Metadata()
		entryRelativePath := relativePath + "/" + e.Name

		var de *dirEntry

		var hash uint64

		switch entry := entry.(type) {
		case fs.Directory:
			oid, h, wasCached, err := u.uploadDirInternal(result, entry, entryRelativePath, hcw, hcr, false)
			if err != nil {
				return repo.NullObjectID, 0, false, err
			}
			//log.Printf("dirHash: %v %v", fullPath, h)
			hash = h
			allCached = allCached && wasCached
			de = newDirEntry(e, oid)

		case fs.Symlink:
			l, err := entry.Readlink()
			if err != nil {
				return repo.NullObjectID, 0, false, err
			}

			de = newDirEntry(e, repo.InlineObjectID([]byte(l)))
			hash = metadataHash(e)

		case *bundle:
			// See if we had this name during previous pass.
			cachedEntry := hcr.findEntry(entryRelativePath)

			// ... and whether file metadata is identical to the previous one.
			cacheMatches := (cachedEntry != nil) && cachedEntry.Hash == bundleHash(entry)

			allCached = allCached && cacheMatches
			childrenMetadata := make([]*dirEntry, len(entry.files))
			for i, f := range entry.files {
				childrenMetadata[i] = newDirEntry(f.Metadata(), repo.NullObjectID)
			}

			if cacheMatches {
				result.Stats.CachedFiles++
				// Avoid hashing by reusing previous object ID.
				de = newDirEntry(e, cachedEntry.ObjectID)
				de.BundledChildren = childrenMetadata
				hash = cachedEntry.Hash
			} else {
				result.Stats.NonCachedFiles++
				de, hash, err = u.uploadBundleInternal(entry)
				if err != nil {
					return repo.NullObjectID, 0, false, fmt.Errorf("unable to hash file: %s", err)
				}
			}

		case fs.File:
			// regular file
			// See if we had this name during previous pass.
			cachedEntry := hcr.findEntry(entryRelativePath)

			// ... and whether file metadata is identical to the previous one.
			computedHash := metadataHash(e)
			cacheMatches := (cachedEntry != nil) && cachedEntry.Hash == computedHash

			allCached = allCached && cacheMatches

			if cacheMatches {
				result.Stats.CachedFiles++
				// Avoid hashing by reusing previous object ID.
				de = newDirEntry(e, cachedEntry.ObjectID)
				hash = cachedEntry.Hash
			} else {
				result.Stats.NonCachedFiles++
				de, hash, err = u.uploadFileInternal(entry, entryRelativePath, false)
				if err != nil {
					return repo.NullObjectID, 0, false, fmt.Errorf("unable to hash file: %s", err)
				}
			}

		default:
			return repo.NullObjectID, 0, false, fmt.Errorf("file type %v not supported", entry.Metadata().Type)
		}

		if hash != 0 {
			dirHasher.Write([]byte(de.Name))
			dirHasher.Write([]byte{0})
			binary.Write(dirHasher, binary.LittleEndian, hash)
		}

		if err := dw.WriteEntry(de); err != nil {
			return repo.NullObjectID, 0, false, err
		}

		if de.Type != fs.EntryTypeDirectory && de.ObjectID.StorageBlock != "" {
			if err := hcw.WriteEntry(hashCacheEntry{
				Name:     entryRelativePath,
				Hash:     hash,
				ObjectID: de.ObjectID,
			}); err != nil {
				return repo.NullObjectID, 0, false, err
			}
		}
	}

	dw.Finalize()

	var directoryOID repo.ObjectID
	dirHash := dirHasher.Sum64()

	cacheddirEntry := hcr.findEntry(relativePath + "/")
	allCached = allCached && cacheddirEntry != nil && cacheddirEntry.Hash == dirHash

	if allCached {
		// Avoid hashing directory listing if every entry matched the cache.
		result.Stats.CachedDirectories++
		directoryOID = cacheddirEntry.ObjectID
	} else {
		result.Stats.NonCachedDirectories++
		directoryOID, err = writer.Result(forceStored)
		if err != nil {
			return directoryOID, 0, false, err
		}
	}

	if directoryOID.StorageBlock != "" {
		if err := hcw.WriteEntry(hashCacheEntry{
			Name:     relativePath + "/",
			ObjectID: directoryOID,
			Hash:     dirHash,
		}); err != nil {
			return repo.NullObjectID, 0, false, err
		}
	}

	// log.Printf("Dir: %v %v %v %v", relativePath, directoryOID.UIString(), dirHash, allCached)
	return directoryOID, dirHash, allCached, nil
}

func (u *uploader) bundleEntries(entries fs.Entries) fs.Entries {
	var bundleMap map[int]*bundle

	result := entries[:0]

	for _, e := range entries {
		switch e := e.(type) {
		case fs.File:
			md := e.Metadata()
			bundleNo := u.getBundleNumber(md)
			if bundleNo != 0 {
				// See if we already started this bundle.
				b := bundleMap[bundleNo]
				if b == nil {
					if bundleMap == nil {
						bundleMap = make(map[int]*bundle)
					}

					bundleMetadata := &fs.EntryMetadata{
						Name: fmt.Sprintf("bundle-%v", bundleNo),
						Type: entryTypeBundle,
					}

					b = &bundle{
						metadata: bundleMetadata,
					}
					bundleMap[bundleNo] = b

					// Add the bundle instead of an entry.
					result = append(result, b)
				}

				// Append entry to the bundle.
				b.append(e)

			} else {
				// Append original entry
				result = append(result, e)
			}

		default:
			// Append original entry
			result = append(result, e)
		}
	}

	return result
}

func (u *uploader) getBundleNumber(md *fs.EntryMetadata) int {
	if md.FileMode().IsRegular() && md.FileSize < maxBundleFileSize {
		return md.ModTime.Year()*100 + int(md.ModTime.Month())
	}

	return 0
}
