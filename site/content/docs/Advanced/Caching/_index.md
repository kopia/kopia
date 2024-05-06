---
title: "Caching"
linkTitle: "Caching"
weight: 50
---

## Caching

To optimize performance Kopia maintains local cache of contents, metadata, indexes and more. This document provides more insight into this process.

### Cache Directory Location

Default Cache Directory Location varies by operating system:

* On Linux - `~/.cache/kopia/{unique-id}`
* On macOS - `~/Library/Caches/kopia/{unique-id}`
* On Windows - `%LocalAppData%\kopia\{unique-id}`

Where `{unique-id}` is a hash of configuration file used and unique identifier that's specific to a
connected repository.

The cache directory location can be overridden when connecting to a repository by specifying `--cache-directory` flag or `KOPIA_CACHE_DIRECTORY` environment variable. It will be persisted in the configuration file.

When set `KOPIA_CACHE_DIRECTORY` environment variable takes precedence over location stored in the configuration file.

### Cache Types

Kopia maintains several different types of caches for different purposes:

* `metadata` (encrypted in storage) - stores directory listings, manifests and other data stored in `q` blobs in the repository. Each cache entry stores the whole original `q` blob (usually ~20 MB each).

* `contents` (encrypted in storage) - stores contents from `p` blobs. Unlike the metadata cache, elements of the contents cache store sections of the underlying blobs.

* `indexes` (non-encrypted) - contains locally-cached index information (mapping of contents to their location in blobs) in a format that can be directly memory-mapped for efficient access.

* `own-writes` (non-encrypted) - keeps track of names and timestamps of index blobs written by local Kopia to ensure it can see its own writes, even if the underlying storage list mechanism is eventually consistent and written files show up after some delay.

* `blob-list` (non-encrypted) - short-lived cache (by default 30 seconds) to avoid frequent listing of blobs in the underlying storage

* `server-contents` (encrypted locally) - contents downloaded from the repository server.

### Setting Cache Parameters

You can override cache sizes, durations and locations by using `kopia cache set`:

```
# set size to 20GB and override location
$ kopia cache set --content-cache-size-mb=20000 --cache-directory=/var/my-cache
```

To override `blob-list` cache duration:

```
$ kopia cache set --max-list-cache-duration=300s
```

Note the cache sizes are not hard limits: cache is swept periodically (every few minutes) to bring
the total usage below the defined limit by removing least-recently used cache items.

A hard limit can be set if required via the corresponding `limit` flag:
```
# set the maximum content cache size to 30GB
$ kopia cache set --content-cache-size-limit-mb=30000
# set the maximum metadata cache size to 20GB
$ kopia cache set --metadata-cache-size-limit-mb=20000
```

### Clearing Cache

Cache can be cleared on demand by `kopia cache clear` or by simply removing appropriate files. It is always safe to remove files from cache.



