package format

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/cachedir"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

// DefaultRepositoryBlobCacheDuration is the duration for which we treat cached kopia.repository
// as valid.
const DefaultRepositoryBlobCacheDuration = 15 * time.Minute

var log = logging.Module("kopia/repo/format")

func formatBytesCachingEnabled(cacheDirectory string, validDuration time.Duration) bool {
	if cacheDirectory == "" {
		return false
	}

	return validDuration > 0
}

func readRepositoryBlobBytesFromCache(ctx context.Context, cachedFile string, validDuration time.Duration) (data []byte, cacheMTime time.Time, err error) {
	cst, err := os.Stat(cachedFile)
	if err != nil {
		return nil, time.Time{}, errors.Wrap(err, "unable to open cache file")
	}

	cacheMTime = cst.ModTime()
	if clock.Now().Sub(cacheMTime) > validDuration {
		// got cached file, but it's too old, remove it
		if err = os.Remove(cachedFile); err != nil {
			log(ctx).Debugf("unable to remove cache file: %v", err)
		}

		return nil, time.Time{}, errors.Errorf("cached file too old")
	}

	data, err = os.ReadFile(cachedFile) // nolint:gosec
	if err != nil {
		return nil, time.Time{}, errors.Wrapf(err, "failed to read the cache file %q", cachedFile)
	}

	return data, cacheMTime, nil
}

// ReadAndCacheRepositoryBlobBytes reads the provided blob from the repository or cache directory.
func ReadAndCacheRepositoryBlobBytes(ctx context.Context, st blob.Storage, cacheDirectory, blobID string, validDuration time.Duration) ([]byte, time.Time, error) {
	cachedFile := filepath.Join(cacheDirectory, blobID)

	if validDuration == 0 {
		validDuration = DefaultRepositoryBlobCacheDuration
	}

	if cacheDirectory != "" {
		if err := os.MkdirAll(cacheDirectory, cache.DirMode); err != nil && !os.IsExist(err) {
			log(ctx).Errorf("unable to create cache directory: %v", err)
		}
	}

	cacheEnabled := formatBytesCachingEnabled(cacheDirectory, validDuration)
	if cacheEnabled {
		data, cacheMTime, err := readRepositoryBlobBytesFromCache(ctx, cachedFile, validDuration)
		if err == nil {
			log(ctx).Debugf("%s retrieved from cache", blobID)

			return data, cacheMTime, nil
		}

		if os.IsNotExist(err) {
			log(ctx).Debugf("%s could not be fetched from cache: %v", blobID, err)
		}
	} else {
		log(ctx).Debugf("%s cache not enabled", blobID)
	}

	var b gather.WriteBuffer
	defer b.Close()

	if err := st.GetBlob(ctx, blob.ID(blobID), 0, -1, &b); err != nil {
		return nil, time.Time{}, errors.Wrapf(err, "error getting %s blob", blobID)
	}

	if cacheEnabled {
		if err := atomicfile.Write(cachedFile, b.Bytes().Reader()); err != nil {
			log(ctx).Warnf("unable to write cache: %v", err)
		}
	}

	return b.ToByteSlice(), clock.Now(), nil
}

// ReadAndCacheDecodedRepositoryConfig reads `kopia.repository` blob, potentially from cache and decodes it.
func ReadAndCacheDecodedRepositoryConfig(ctx context.Context, st blob.Storage, password, cacheDir string, validDuration time.Duration) (ufb *DecodedRepositoryConfig, err error) {
	ufb = &DecodedRepositoryConfig{}

	ufb.KopiaRepositoryBytes, ufb.CacheMTime, err = ReadAndCacheRepositoryBlobBytes(ctx, st, cacheDir, KopiaRepositoryBlobID, validDuration)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read format blob")
	}

	if err = cachedir.WriteCacheMarker(cacheDir); err != nil {
		return nil, errors.Wrap(err, "unable to write cache directory marker")
	}

	ufb.KopiaRepository, err = ParseKopiaRepositoryJSON(ufb.KopiaRepositoryBytes)
	if err != nil {
		return nil, errors.Wrap(err, "can't parse format blob")
	}

	ufb.KopiaRepositoryBytes, err = addFormatBlobChecksumAndLength(ufb.KopiaRepositoryBytes)
	if err != nil {
		return nil, errors.Errorf("unable to add checksum")
	}

	ufb.FormatEncryptionKey, err = ufb.KopiaRepository.DeriveFormatEncryptionKeyFromPassword(password)
	if err != nil {
		return nil, err
	}

	ufb.RepoConfig, err = ufb.KopiaRepository.DecryptRepositoryConfig(ufb.FormatEncryptionKey)
	if err != nil {
		return nil, ErrInvalidPassword
	}

	return ufb, nil
}

// ReadAndCacheRepoUpgradeLock loads the lock config from cache and returns it.
func ReadAndCacheRepoUpgradeLock(ctx context.Context, st blob.Storage, password, cacheDir string, validDuration time.Duration) (*UpgradeLockIntent, error) {
	ufb, err := ReadAndCacheDecodedRepositoryConfig(ctx, st, password, cacheDir, validDuration)
	return ufb.RepoConfig.UpgradeLock, err
}
