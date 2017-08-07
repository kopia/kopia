package snapshot

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/dir"
	"github.com/kopia/kopia/internal/hashcache"
	"github.com/kopia/kopia/repo"
)

const (
	maxBundleFileSize = 65536

	maxFlatBundleSize   = 1000000
	maxYearlyBundleSize = 5000000
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

func bundleHash(b *dir.Bundle) uint64 {
	h := fnv.New64a()
	hashEntryMetadata(h, b.Metadata())
	for i, f := range b.Files {
		binary.Write(h, binary.LittleEndian, i)
		hashEntryMetadata(h, f.Metadata())
	}
	return h.Sum64()
}

// uploadContext supports efficient uploading files and directories to repository.
type uploadContext struct {
	ctx context.Context

	repo        *repo.Repository
	cacheWriter *hashcache.Writer
	cacheReader *hashcache.Reader

	stats    *Stats
	progress UploadProgress

	uploadBuf []byte

	Cancelled bool
}

func uploadFileInternal(u *uploadContext, f fs.File, relativePath string, forceStored bool) (*dir.Entry, uint64, error) {
	u.progress.Started(relativePath, f.Metadata().FileSize)

	file, err := f.Open()
	if err != nil {
		return nil, 0, fmt.Errorf("unable to open file: %v", err)
	}
	defer file.Close()

	writer := u.repo.NewWriter(repo.WriterOptions{
		Description: "FILE:" + f.Metadata().Name,
	})
	defer writer.Close()

	written, err := copyWithProgress(u, relativePath, writer, file, 0, f.Metadata().FileSize)
	if err != nil {
		u.progress.Finished(relativePath, f.Metadata().FileSize, err)
		return nil, 0, err
	}

	e2, err := file.EntryMetadata()
	if err != nil {
		u.progress.Finished(relativePath, f.Metadata().FileSize, err)
		return nil, 0, err
	}

	r, err := writer.Result(forceStored)
	if err != nil {
		u.progress.Finished(relativePath, f.Metadata().FileSize, err)
		return nil, 0, err
	}

	de := newDirEntry(e2, r)
	de.FileSize = written

	u.progress.Finished(relativePath, f.Metadata().FileSize, nil)

	return de, metadataHash(&de.EntryMetadata), nil
}

func copyWithProgress(u *uploadContext, path string, dst io.Writer, src io.Reader, completed int64, length int64) (int64, error) {
	if u.uploadBuf == nil {
		u.uploadBuf = make([]byte, 128*1024) // 128 KB buffer
	}

	var written int64

	for {
		readBytes, readErr := src.Read(u.uploadBuf)
		if readBytes > 0 {
			wroteBytes, writeErr := dst.Write(u.uploadBuf[0:readBytes])
			if wroteBytes > 0 {
				written += int64(wroteBytes)
				completed += int64(wroteBytes)
				if length < completed {
					length = completed
				}
				u.progress.Progress(path, completed, length)
			}
			if writeErr != nil {
				return written, writeErr
			}
			if readBytes != wroteBytes {
				return written, io.ErrShortWrite
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}

			return written, readErr
		}
	}

	return written, nil
}

func newDirEntry(md *fs.EntryMetadata, oid repo.ObjectID) *dir.Entry {
	return &dir.Entry{
		EntryMetadata: *md,
		ObjectID:      oid,
	}
}

func uploadBundleInternal(u *uploadContext, b *dir.Bundle, relativePath string) (*dir.Entry, uint64, error) {
	bundleMetadata := b.Metadata()
	u.progress.Started(relativePath, b.Metadata().FileSize)

	writer := u.repo.NewWriter(repo.WriterOptions{
		Description: "BUNDLE:" + bundleMetadata.Name,
	})

	defer writer.Close()

	var uploadedFiles []fs.File
	var err error

	de := newDirEntry(bundleMetadata, repo.NullObjectID)
	var totalBytes int64

	for _, fileEntry := range b.Files {
		file, err := fileEntry.Open()
		if err != nil {
			u.progress.Finished(relativePath, totalBytes, err)
			return nil, 0, err
		}

		fileMetadata, err := file.EntryMetadata()
		if err != nil {
			u.progress.Finished(relativePath, totalBytes, err)
			return nil, 0, err
		}

		written, err := copyWithProgress(u, relativePath, writer, file, totalBytes, b.Metadata().FileSize)
		if err != nil {
			u.progress.Finished(relativePath, totalBytes, err)
			return nil, 0, err
		}

		fileMetadata.FileSize = written
		de.BundledChildren = append(de.BundledChildren, newDirEntry(fileMetadata, repo.NullObjectID))

		uploadedFiles = append(uploadedFiles, dir.NewBundledFile(fileMetadata))
		totalBytes += written
		file.Close()
	}

	b.Files = uploadedFiles
	de.ObjectID, err = writer.Result(true)
	de.FileSize = totalBytes
	if err != nil {
		u.progress.Finished(relativePath, totalBytes, err)
		return nil, 0, err
	}

	u.progress.Finished(relativePath, totalBytes, nil)
	return de, bundleHash(b), nil
}

// uploadFile uploads the specified File to the repository.
func uploadFile(u *uploadContext, file fs.File) (repo.ObjectID, error) {
	e, _, err := uploadFileInternal(u, file, file.Metadata().Name, true)
	if err != nil {
		return repo.NullObjectID, err
	}
	return e.ObjectID, nil
}

// uploadDir uploads the specified Directory to the repository.
// An optional ID of a hash-cache object may be provided, in which case the uploadContext will use its
// contents to avoid hashing
func uploadDir(u *uploadContext, dir fs.Directory) (repo.ObjectID, repo.ObjectID, error) {
	var err error

	if err := u.repo.BeginPacking(); err != nil {
		return repo.NullObjectID, repo.NullObjectID, err
	}

	mw := u.repo.NewWriter(repo.WriterOptions{
		Description:     "HASHCACHE:" + dir.Metadata().Name,
		BlockNamePrefix: "H",
	})
	defer mw.Close()
	u.cacheWriter = hashcache.NewWriter(mw)
	oid, _, _, err := uploadDirInternal(u, dir, ".", true)
	u.cacheWriter.Finalize()
	u.cacheWriter = nil

	if err != nil {
		return repo.NullObjectID, repo.NullObjectID, err
	}

	hcid, err := mw.Result(true)
	if err := u.repo.FinishPacking(); err != nil {
		return repo.NullObjectID, repo.NullObjectID, fmt.Errorf("can't finish packing: %v", err)
	}
	return oid, hcid, err
}

func uploadDirInternal(
	u *uploadContext,
	directory fs.Directory,
	relativePath string,
	forceStored bool,
) (repo.ObjectID, uint64, bool, error) {
	u.progress.StartedDir(relativePath)
	defer u.progress.FinishedDir(relativePath)

	u.stats.TotalDirectoryCount++

	entries, err := directory.Readdir()
	if err != nil {
		return repo.NullObjectID, 0, false, err
	}

	entries = bundleEntries(u, entries)

	writer := u.repo.NewWriter(repo.WriterOptions{
		Description: "DIR:" + relativePath,
	})

	dw := dir.NewWriter(writer)
	defer writer.Close()

	allCached := true

	dirHasher := fnv.New64a()
	dirHasher.Write([]byte(relativePath))
	dirHasher.Write([]byte{0})

	for _, entry := range entries {
		e := entry.Metadata()
		entryRelativePath := relativePath + "/" + e.Name

		var de *dir.Entry

		var hash uint64

		switch entry := entry.(type) {
		case fs.Directory:
			oid, h, wasCached, err := uploadDirInternal(u, entry, entryRelativePath, false)
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

		case *dir.Bundle:
			// See if we had this name during previous pass.
			cachedEntry := u.cacheReader.FindEntry(entryRelativePath)

			// ... and whether file metadata is identical to the previous one.
			cacheMatches := (cachedEntry != nil) && cachedEntry.Hash == bundleHash(entry)

			allCached = allCached && cacheMatches
			childrenMetadata := make([]*dir.Entry, len(entry.Files))
			for i, f := range entry.Files {
				childrenMetadata[i] = newDirEntry(f.Metadata(), repo.NullObjectID)
			}

			if cacheMatches {
				u.stats.CachedFiles++
				u.progress.Cached(entryRelativePath, entry.Metadata().FileSize)
				// Avoid hashing by reusing previous object ID.
				de = newDirEntry(e, cachedEntry.ObjectID)
				de.BundledChildren = childrenMetadata
				hash = cachedEntry.Hash
			} else {
				u.stats.NonCachedFiles++
				de, hash, err = uploadBundleInternal(u, entry, entryRelativePath)
				if err != nil {
					return repo.NullObjectID, 0, false, fmt.Errorf("unable to hash file: %s", err)
				}
			}
			u.stats.TotalBundleCount++
			u.stats.TotalBundleSize += de.FileSize
			u.stats.TotalFileSize += de.FileSize

		case fs.File:
			// regular file
			// See if we had this name during previous pass.
			cachedEntry := u.cacheReader.FindEntry(entryRelativePath)

			// ... and whether file metadata is identical to the previous one.
			computedHash := metadataHash(e)
			cacheMatches := (cachedEntry != nil) && cachedEntry.Hash == computedHash

			allCached = allCached && cacheMatches

			if cacheMatches {
				u.stats.CachedFiles++
				u.progress.Cached(entryRelativePath, entry.Metadata().FileSize)
				// Avoid hashing by reusing previous object ID.
				de = newDirEntry(e, cachedEntry.ObjectID)
				hash = cachedEntry.Hash
			} else {
				u.stats.NonCachedFiles++
				de, hash, err = uploadFileInternal(u, entry, entryRelativePath, false)
				if err != nil {
					return repo.NullObjectID, 0, false, fmt.Errorf("unable to hash file: %s", err)
				}
			}

			u.stats.TotalFileCount++
			u.stats.TotalFileSize += de.FileSize

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
			if err := u.cacheWriter.WriteEntry(hashcache.Entry{
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

	cacheddirEntry := u.cacheReader.FindEntry(relativePath + "/")
	allCached = allCached && cacheddirEntry != nil && cacheddirEntry.Hash == dirHash

	if allCached {
		// Avoid hashing directory listing if every entry matched the cache.
		u.stats.CachedDirectories++
		directoryOID = cacheddirEntry.ObjectID
	} else {
		u.stats.NonCachedDirectories++
		directoryOID, err = writer.Result(forceStored)
		if err != nil {
			return directoryOID, 0, false, err
		}
	}

	if directoryOID.StorageBlock != "" {
		if err := u.cacheWriter.WriteEntry(hashcache.Entry{
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

func bundleEntries(u *uploadContext, entries fs.Entries) fs.Entries {
	var bundleMap map[int]*dir.Bundle

	result := entries[:0]

	var totalLength int64
	totalLengthByYear := map[int]int64{}
	totalLengthByMonth := map[int]int64{}
	var yearlyMax int64

	for _, e := range entries {
		switch e := e.(type) {
		case fs.File:
			md := e.Metadata()
			if md.FileMode().IsRegular() && md.FileSize < maxBundleFileSize {
				totalLength += md.FileSize
				totalLengthByMonth[md.ModTime.Year()*100+int(md.ModTime.Month())] += md.FileSize
				totalLengthByYear[md.ModTime.Year()] += md.FileSize
				if totalLengthByYear[md.ModTime.Year()] > yearlyMax {
					yearlyMax = totalLengthByYear[md.ModTime.Year()]
				}
			}
		}
	}

	var getBundleNumber func(md *fs.EntryMetadata) int

	switch {
	case totalLength < maxFlatBundleSize:
		getBundleNumber = getFlatBundleNumber

	case maxYearlyBundleSize < maxYearlyBundleSize:
		getBundleNumber = getYearlyBundleNumber

	default:
		getBundleNumber = getMonthlyBundleNumber
	}

	for _, e := range entries {
		switch e := e.(type) {
		case fs.File:
			md := e.Metadata()
			bundleNo := getBundleNumber(md)
			if bundleNo != 0 {
				// See if we already started this bundle.
				b := bundleMap[bundleNo]
				if b == nil {
					if bundleMap == nil {
						bundleMap = make(map[int]*dir.Bundle)
					}

					bundleMetadata := &fs.EntryMetadata{
						Name: fmt.Sprintf("bundle-%v", bundleNo),
						Type: dir.EntryTypeBundle,
					}

					b = dir.NewBundle(bundleMetadata)
					bundleMap[bundleNo] = b

					// Add the bundle instead of an entry.
					result = append(result, b)
				}

				// Append entry to the bundle.
				b.Append(e)

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

func getMonthlyBundleNumber(md *fs.EntryMetadata) int {
	if md.FileMode().IsRegular() && md.FileSize < maxBundleFileSize {
		return md.ModTime.Year()*100 + int(md.ModTime.Month())
	}

	return 0
}

func getYearlyBundleNumber(md *fs.EntryMetadata) int {
	if md.FileMode().IsRegular() && md.FileSize < maxBundleFileSize {
		return md.ModTime.Year() * 100
	}

	return 0
}

func getFlatBundleNumber(md *fs.EntryMetadata) int {
	if md.FileMode().IsRegular() && md.FileSize < maxBundleFileSize {
		return 1
	}

	return 0
}

// Upload uploads contents of the specified filesystem entry (file or directory) to the repository and updates given manifest with statistics.
// Old snapshot manifest, when provided can be used to speed up backups by utilizing hash cache.
func Upload(
	ctx context.Context,
	repository *repo.Repository,
	source fs.Entry,
	sourceInfo *SourceInfo,
	policy FilesPolicy,
	old *Manifest,
	progress UploadProgress,
) (*Manifest, error) {
	if progress == nil {
		progress = &nullUploadProgress{}
	}

	u := &uploadContext{
		ctx:         ctx,
		repo:        repository,
		cacheReader: &hashcache.Reader{},
		stats:       &Stats{},
		progress:    progress,
	}

	s := &Manifest{
		Source: *sourceInfo,
	}

	if old != nil {
		if r, err := u.repo.Open(old.HashCacheID); err == nil {
			u.cacheReader.Open(r)
		}
	}

	var err error

	s.StartTime = time.Now()

	switch entry := source.(type) {
	case fs.Directory:
		s.RootObjectID, s.HashCacheID, err = uploadDir(u, entry)

	case fs.File:
		s.RootObjectID, err = uploadFile(u, entry)

	default:
		return nil, fmt.Errorf("unsupported source: %v", s.Source)
	}
	if err != nil {
		return nil, err
	}

	s.EndTime = time.Now()
	s.Stats = *u.stats
	s.Stats.Repository = u.repo.Status().Stats

	return s, nil
}
