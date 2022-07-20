---
title: "Amazon S3"
linkTitle: "Amazon S3"
weight: 20
unlisted: true
---

[Amazon S3](https://aws.amazon.com/s3/) is a highly featured cloud storage option. Kopia supports [connecting](/docs/reference/command-line/common/repository-create-s3/) to a repository stored on Amazon S3.

Amazon S3 supports various types of [storage classes](https://aws.amazon.com/s3/storage-classes/) for storing data. Each class trades some benefits with certain drawbacks. For example, comparing to the Standard storage class, Standard-Infrequent Access has lower [cost](https://aws.amazon.com/s3/pricing/) for storage, but higher cost for requests, making it ideal for "long-term storage, backups, and as a data store for disaster recovery files".

By default, all Kopia blobs are uploaded and retrieved in the Standard storage class. In order to use other classes, one approach is to create [Lifecycle rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html) in the bucket that move objects in between. This approach could be beneficial in many ways.

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
