---
title: "Storage Tiers"
linkTitle: "Storage Tiers"
weight: 55
---

If you are paying for storage from a cloud storage provider (see [Repositories](/docs/repositories/) for a list of currently supported storage backends), then a consideration is which access tier, or tiers, to choose from that provider. Providers may offer a range of tiers suitable for different use cases and may, for example, offer cheaper storage tiers for content that is written but read comparatively rarely.

Choosing the correct tier according to the data access pattern is very important -- while a "cold" storage tier may be cheaper, data transfer/retrieval costs or minimum data retention periods can be result in quite high overall costs when you need to retrieve a lot of data.

A Kopia repository holds a number of different file types, identified by their filename prefix character, with differing access patterns:

* `p` blobs -- store the bulk of data -- mostly write-only, except during compactions as part of [full maintenance](/docs/advanced/maintenance).
* `q` blobs -- store the metadata (directory listings, manifests, etc.) -- frequently read and written.
* `n`, `m`, `l` and `x` blobs -- indexes -- frequently read, written and deleted.
* `s` session blobs -- frequently read, written and deleted.

The recommendation is to put everything in a hot/warm storage tier, except possibly for `p` blobs, which can be put in cold(er) storage because they are infrequently accessed.

For AWS, this could be accomplished with a lifecycle rule similar to the one below.

```
<LifecycleConfiguration>
  <Rule>
    <ID>Store p blobs in Glacier</ID>
    <Filter>
       <Prefix>p</Prefix>
    </Filter>
    <Status>Enabled</Status>
    <Transition>
      <Days>0</Days>
      <StorageClass>S3 Glacier</StorageClass>
    </Transition>
  </Rule>
</LifecycleConfiguration>
```

In this example, all `p` blobs would be transitioned to Glacier storage at midnight UTC of the day following creation.

It is very important to note that if this lifecycle is deployed, it would be prudent to disable both forms of maintenance on the Kopia repository, so as to avoid unexpected charges. This can be done with two commands from the CLI:

* `kopia maintenance set --enable-quick=false`
* `kopia maintenance set --enable-full=false`

A benefit of this deployment is that the Kopia repository could then be made immutable using [S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html), thus enforcing a WORM policy on the repository.

You can see when files are accessed by viewing Kopia debug logs (`kopia ... --log-level=debug`) -- look for lines containing `STORAGE`.

Note that `q` blobs are very aggressively cached by the Kopia client, so may appear not to be accessed when performing basic operations like listing snapshots etc.