Kopia Repository
=====

![Kopia](kopia.svg)

[![Build Status](https://travis-ci.org/kopia/repo.svg?branch=master)](https://travis-ci.org/kopia/repo)
[![GoDoc](https://godoc.org/github.com/kopia/repo?status.svg)](https://godoc.org/github.com/kopia/repo)
[![Coverage Status](https://coveralls.io/repos/github/kopia/repo/badge.svg?branch=master)](https://coveralls.io/github/kopia/repo?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/kopia/repo)](https://goreportcard.com/report/github.com/kopia/repo)

Features
---

Kppia Repository organizes raw blob storage, such as Google Cloud Storage or Amazon S3 buckets into content-addressable storage with:

* deduplication
* client-side encryption
* caching
* object splitting and merging
* packaging and indexing (organizing many small objects into larger ones)
* shared access from multiple computers
* simple manifest management for storing label-addressable content

All Repository features are implemented client-side, without any need for a custom server, thus encryption keys never leave the client.

The primary user of Repository is [Kopia](https://github.com/kopia/kopia) which stores its filesystem snapshots in content-addressable storage, but Repository is designed to be a general-purpose storage system.

Repository implements 4 storage layers:

* [Object Storage](https://godoc.org/github.com/kopia/repo/object) for storing objects of arbitrary size with encryption and deduplication
* [Manifest Storage](https://godoc.org/github.com/kopia/repo/manifest) for storing small JSON-based manifests indexed by arbitrary labels (`key=value`)
* [Block Storage](https://godoc.org/github.com/kopia/repo/block) for storing content-addressable, indivisible blocks of relatively small sizes (up to 10-20MB each) with encryption and deduplication
* [Raw BLOB storage](https://godoc.org/github.com/kopia/repo/storage) provides raw access to physical blocks

Usage
---

Initialize repository in a given storage (this is done only once).

```golang
// connect to a Google Cloud Storage blucket.
st, err := gcs.New(ctx, &gcs.Options{
  Bucket: "my-bucket",
})
password := "my-super-secret-password"
if err := repo.Initialize(ctx, st, &repo.NewRepositoryOptions{
  BlockFormat: block.FormattingOptions{
    Hash:       "HMAC-SHA256-128",
    Encryption: "AES-256-CTR",
  },
}, password); err != nil {
  log.Fatalf("unable to initialize repository: %v", err)
}
```

Now connect to repository, which creates a local configuration file that persists all connection details.

```golang
configFile := "/tmp/my-repo.config"
if err := repo.Connect(ctx, configFile, st, password, repo.ConnectOptions{
  CachingOptions: block.CachingOptions{
  CacheDirectory:    cacheDirectory,
  MaxCacheSizeBytes: 100000000,
},
}); err != nil {
  log.Fatalf("unable to connect to repository: %v", err)
}
```

To open repository use:

```golang
ctx := context.Background()
rep, err := repo.Open(ctx, configFile, password, nil)
if err != nil {
  log.Fatalf("unable to open the repository: %v", err)
}

// repository must be closed at the end.
defer rep.Close(ctx)
```

Writing objects:

```golang

w := rep.Objects.NewWriter(ctx, object.WriterOptions{})
defer w.Close()

// w implements io.Writer
fmt.Fprintf(w, "hello world")

// Object ID is a function of contents written, so every time we write "hello world" we're guaranteed to get exactly the same ID.
objectID, err := w.Result()
if err != nil {
  log.Fatalf("upload failed: %v", err)
}
```

Reading objects:

```golang
rd, err := rep.Objects.Open(ctx, objectID)
if err != nil {
  log.Fatalf("open failed: %v", err)
}
defer rd.Close()

data, err := ioutil.ReadAll(rd)
if err != nil {
  log.Fatalf("read failed: %v", err)
}

// Outputs "hello world"
log.Printf("data: %v", string(data))
```

Saving manifest with a given set of labels:

```golang
labels := map[string]string{
  "type": "custom-object",
  "my-kind": "greeting",
}

payload := map[string]string{
  "myObjectID": objectID,
}

manifestID, err := rep.Manifests.Put(ctx, labels, payload)
if err != nil {
  log.Fatalf("manifest put failed: %v", err)
}

log.Printf("saved manifest %v", manifestID)
```

Loading manifests matching labels:

```golang
manifests, err := rep.Manifests.Find(ctx, labels)
if err != nil {
  log.Fatalf("unable to find manifests: %v", err)
}
for _, m := range manifests {
  var val map[string]string

  if err := rep.Manifests.Get(ctx, m.ID, &val); err != nil {
    log.Fatalf("unable to load manfiest %v: %v", m.ID, err)
  }

  log.Printf("loaded manifest: %v created at %v", val["myObjectID"], m.ModTime)
}
```


FAQ
---

1. How stable is it?

This library is still in development and is **not ready for general use**.

The repository data format is still subject to change, including backwards-incompatible changes, which will require data migration, although at some point before v1.0 we will declare the format to be stable and will maintain backward compatibility going forward.

2. How big can a repository get?

There's no inherent size limit, but a rule of thumb should be no more than `10 TB` (at least for now, until we test with larger repositories).

The data is efficiently packed into a small number of files and stored, but indexes need to be cached locally and will consume disk space and RAM.

>For example:
>
>One sample repository of `480 GB` of data from home NAS containing a mix of photos, videos, documents and music files contains:
> * `1874361` content-addressable blocks/objects
> * `27485` physical objects (packs) in cloud storage bucket (typically between 20MB and 30MB each)
> *  `70 MB` of indexes

3. How safe is the data?

Your data can only be as safe as the underlying storage, so it's recommended to use one of high-quality cloud storage solutions, which nowadays provide very high-durability, high-throughput and low-latency for access to your data at a very reasonable price.

In addition to that, Kopia employs several data protection techniques, such as encryption, checksumming to detect accidental bit flips, redundant storage of indexes, and others.

> **WARNING: It's not recommended to trust all your data to Kopia just yet - always have another backup**.

4. I'd like to contribute

Sure, get started by [filing an Issue](https://github.com/kopia/repo/issues) or sending a Pull request.

5. I found a security issue

Please notify us privately at `jaak@jkowalski.net` so we can work on addressing the issue and releasing a patch.

Licensing
---
Kopia is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full license text.

Disclaimer
---

Kopia is a personal project and is not affiliated with, supported or endorsed by Google.

Cryptography Notice
---

  This distribution includes cryptographic software. The country in
  which you currently reside may have restrictions on the import,
  possession, use, and/or re-export to another country, of encryption
  software. BEFORE using any encryption software, please check your
  country's laws, regulations and policies concerning the import,
  possession, or use, and re-export of encryption software, to see if
  this is permitted. See <http://www.wassenaar.org/> for more
  information.

  The U.S. Government Department of Commerce, Bureau of Industry and
  Security (BIS), has classified this software as Export Commodity
  Control Number (ECCN) 5D002.C.1, which includes information security
  software using or performing cryptographic functions with symmetric
  algorithms. The form and manner of this distribution makes it
  eligible for export under the License Exception ENC Technology
  Software Unrestricted (TSU) exception (see the BIS Export
  Administration Regulations, Section 740.13) for both object code and
  source code.
