---
title: "Using Different Storage Classes"
linkTitle: "Using Different Storage Classes"
weight: 10
aliases:
    - /advanced/amazon-s3/
---

Most of the [cloud storages supported by Kopia](../repositories/) have a feature called storage classes (or storage tiers). Storage classes allow you to trade-off costs for storage and access and, in some cases, data redundancy. Exactly what the trade-offs are vary between cloud providers. However, 'hot' storage is typically the most expensive to store, the least expensive to access, and has the highest redundancy; 'cold' storage is less expensive to store, more expensive to access, and sometimes has less redundancy than hot storage; 'archive' storage is the least expensive to store, the most expensive to access, often has less redundancy than both hot and cold storage, and most of the time provides access to your files with a delay of several hours (i.e., you cannot instantly download your files). The most famous example of archive storage is Amazon Glacier (now called Amazon Deep Glacier).

> PRO TIP: Kopia works without issue with all storage classes that provide instant access to your files. You can also use Kopia with storage classes that provide delayed access to your files, but some [Kopia features will not work](#archive): Kopia is not designed for storage that does not provide instant access, and it is not recommended for most users because delayed access can cause complications. If you really want to use archive storage, consider using Google Cloud Storage's Archive storage class -- it is more expensive than other archive storage but still very cheap to store ($0.0012 per GB at the time of this writing) and provides instant access to your files, but remeber that, like other archive storage, costs are high for accessing files in Google Cloud Storage's Archive storage class.

By default, Kopia stores all snapshots on whatever is the standard storage class for your cloud provider; often, that is hot storage. If you want to change storage classes, one way is to create 'lifecycle' rules from within your cloud provider account. This lifecycle rules feature allows you to automatically move snapshots to different storage classes directly from within your bucket. This approach is independent from Kopia; Kopia does not manage lifecycle rules for you, you have to do it from within your cloud provider account.

Alternatively, Kopia supports the ability to pick a storage class when uploading snapshots to your cloud provider, so that you do not need to create any lifecycle rules. However, this feature is only available for the [Amazon S3 and S3-compatible cloud storage](../repositories/#amazon-s3-and-s3-compatible-cloud-storage) that have implemented the S3 storage class API (some S3-compatible cloud storage have implemented this API and some have not; you will need to research your cloud provider to find out).

Alternatively, Kopia allows user to upload a `.storageconfig` JSON file to the bucket, which changes how Kopia behaves when operating on that bucket. The configs will affect all the changes after the file is uploaded. It will not retrospectively change data already existed before.

### `.storageconfig`

An example `.storageconfig` looks like this:

```json
{
   "blobOptions": [
     { "prefix": "p", "storageClass": "INTELLIGENT_TIERING" },
     { "prefix": "s", "storageClass": "REDUCED_REDUNDANCY" },
     { "storageClass": "STANDARD_IA" }
  ]
}
```

The `blobOptions` array could have arbitrary number of objects. Each entry contains an optional `prefix` for specifying [target blobs](/docs/advanced/storage-tiers/), and a required `storageClass` for specifying which storage class to use for those blobs. The list of available storage classes can be found [here](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-request-header-StorageClass). This structure is similar to the [.shards](/docs/advanced/sharding/#shards) `overrides` array.

In this example, blobs with ID starting with "p", which store bulks of data, are stored in Intelligent-Tiering. "s" blobs, which store session data, are put in Reduced Redundancy Storage (RRS). The last option without `prefix` defines the storage class for all other types of blobs. Note that the `prefix` does not have to be single character.

In order to use this config before the Kopia repository is created, user has to upload the file to the bucket first, then proceed with `kopia repository create s3` command.

#### Special notes for using the Glacier storage classes

The two Glacier storage classes (with and without "Deep Archive") are special in that they are extremely cheap in storage (Deep Archive is about $1/TB/Month), but very expensive to retrieve data, as they are designed for long-term archive. Also, in order to retrieve, user has to firstly submit a ["Restore" request](https://docs.aws.amazon.com/AmazonS3/latest/userguide/restoring-objects.html), wait for the request to complete (which can take hours depending on cost), then access the data normally. Basically, Glacier is a "write-only" storage.

Because of all the restriction, Glacier is not recommended for most users to use. However, if you know what you are doing exactly, and are willing to venture into the icy realm, here's some critical information.

* Blobs in Glacier can't be directly read, therefore types of blobs required to be read for Kopia to normally function must not be put there. This includes all but the "p" blobs.
* When "p" blobs are put in Glacier, `kopia restore` command involving these blobs will start to fail. User must complete the aforementioned S3 Restore request first.
* Because Kopia's full maintenance will usually compact "p" blobs by reading and re-writing them, running full maintenance with Glacier blobs involved is currently not supported.

### What's next

In theory, all the [request parameters](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax) from S3's `PutObject` can be adopted in `.storageconfig`. For example, the "server side encryption" feature allows user to encrypt objects uploaded to the bucket. However, since Kopia already [encrypts](/docs/advanced/encryption/) blobs before uploading, it seems redundant to support that. Feel free to [let us know](https://github.com/kopia/kopia/issues/new) your ideas of expanding the functionality of this feature.



If you are paying for storage from a cloud storage provider (see [Repositories](/docs/repositories/) for a list of currently supported storage backends), then a consideration is which access tier, or tiers, to choose from that provider. Providers may offer a range of tiers suitable for different use cases and may, for example, offer cheaper storage tiers for content that is written but read comparatively rarely.

Choosing the correct tier according to the data access pattern is very important -- while a "cold" storage tier may be cheaper, data transfer/retrieval costs or minimum data retention periods can be result in quite high overall costs when you need to retrieve a lot of data.

A Kopia repository holds a number of different file types, identified by their filename prefix character, with differing access patterns:

* `p` blobs -- store the bulk of data -- mostly write-only, except during compactions as part of [full maintenance](/docs/advanced/maintenance).
* `q` blobs -- store the metadata (directory listings, manifests, etc.) -- frequently read and written.
* `n`, `m`, `l` and `x` blobs -- indexes -- frequently read, written and deleted.
* `s` session blobs -- frequently read, written and deleted.

The recommendation is to put everything in a hot/warm storage tier, except possibly for `p` blobs, which can be put in cold(er) storage because they are infrequently accessed.

You can see when files are accessed by viewing Kopia debug logs (`kopia ... --log-level=debug`) -- look for lines containing `STORAGE`.

Note that `q` blobs are very aggressively cached by the Kopia client, so may appear not to be accessed when performing basic operations like listing snapshots etc.
