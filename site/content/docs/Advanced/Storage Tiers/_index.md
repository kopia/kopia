---
title: "Using Different Storage Classes"
linkTitle: "Using Different Storage Classes"
weight: 10
aliases:
    - ../advanced/amazon-s3/
---

Most of the [cloud storages supported by Kopia](../../repositories/) have a feature called storage classes (or storage tiers). Storage classes allow you to trade-off costs for storage and access and, in some cases, data redundancy. Exactly what the trade-offs are vary between cloud providers. However, 

* 'hot' storage is typically the most expensive to store, the least expensive to access, and has the highest redundancy; 
* 'cold' storage is less expensive to store, more expensive to access, and sometimes has less redundancy than hot storage; 
* and 'archive' storage is the least expensive to store, the most expensive to access, often has less redundancy than both hot and cold storage, and most of the time provides access to your files with a delay of several hours (i.e., you cannot instantly download your files). 

Some storage classes also have a minimum data retention policy, meaning that they charge you the full price for their whole retention period (e.g. 90 days) even if you delete files before the retention period has ended. This retention policy is typically with cold and archive storage, but some hot storage also have such policies. For example, Wasabi has a 90-day retention policy (at the time of this writing).

The most famous example of archive storage is Amazon Glacier (now called Amazon Glacier Deep Archive).

Kopia works without issue with all storage classes that provide **instant access** to your files, namely hot or cold storage. Kopia will **not** work with archive storage classes that provide *delayed* access to your files, such as Amazon Glacier Deep Archive.

> PRO TIP: If you are not downloading or [testing](../consistency/) your snapshots regularly, you may save money by using Kopia with some sort of cold storage class; just make sure whatever storage class you use, that storage class provides instant access to your files and not delayed access.

By default, Kopia stores all snapshots using whatever is the standard storage class for your cloud provider; often, that is hot storage. If you want to change storage classes, one way is to create 'lifecycle' rules from within your cloud provider account. This lifecycle rules feature allows you to automatically move snapshots to different storage classes directly from within your bucket. This approach is independent from Kopia; Kopia does not manage lifecycle rules for you, you have to do it from within your cloud provider account.

Another approach that some providers allow is to set a default storage class for a bucket that is other than their standard. This approach, too, is independent from and is not managed by Kopia: you control this from within your cloud provider account. If you follow this approach, Kopia will automatically use the default storage class that you set for your bucket.

Alternatively, Kopia supports the ability to pick a storage class when uploading snapshots to your cloud provider, so that you do not need to create any lifecycle rules or change the default storage for your bucket. However, this feature is only available for the [Amazon S3 and S3-compatible cloud storage](../../repositories/#amazon-s3-and-s3-compatible-cloud-storage) that have implemented the S3 storage class API (some S3-compatible cloud storage have implemented this API and some have not; you will need to research your cloud provider to find out). To use this feature, you must upload a `.storageconfig` file to the bucket in your cloud provider account that you use to store Kopia snapshots. This feature tells Kopia what storage class to use when uploading your snapshots.

> PRO TIP: `.storageconfig` works for all uploads after you have created the file; it will not change the storage class of files uploaded before you create `.storageconfig`. Thus, if you want to use `.storageconfig` from all data stored in a repository, make sure to upload `.storageconfig` before you [create the repository in Kopia](../../getting-started/).

An example `.storageconfig` looks like this:

```json
{
   "blobOptions": [
     { "prefix": "p", "storageClass": "STANDARD_IA" },
     { "storageClass": "STANDARD" }
  ]
}
```

In this file, you need to create an object in the `blobOptions` array for each blob type whose storage class you want to change. In the above example, the `.storageconfig` file is telling Kopia to upload `p` blobs using the `STANDARD_IA` storage class and all other blobs as the `STANDARD` storage class. You can add as many objects in the `blobOptions` array as you desire.

> PRO TIP: You can read about all of Amazon S3's storage classes on [Amazon's website](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-request-header-StorageClass). For all S3-compatible storage, you will need to research what storage classes are supported by that provider and what the storage classes are called. Once you know the names of the storage classes, you can use `.storageconfig` as described in this document.

The following are all the blob types that are used by Kopia; you can change the storage class for each of these blob types by creating an object for the respective `prefix` in a `.storageconfig` file:

* `p` blobs store the bulk of snapshot data; they are mostly written once and not accessed again, except during compactions as part of [full maintenance](../maintenance/) or when (optionally) [testing your snapshots](../consistency/)
* `q` blobs store the metadata for your snapshots (directory listings, manifests, etc.); they are frequently read and written
* `n`, `m`, `l` and `x` blobs are for indexes used in your snapshots; they are frequently read, written, and deleted
* `s` blobs are for sessions; they are frequently read, written, and deleted

You may save money (depending on the cloud storage you use and their pricing) by putting `p` blobs in cold storage because those blobs are infrequently accessed. You likely will not be saving money by putting all other blobs in cold storage, since those blobs are frequently accessed. 

> PRO TIP: You can see when files are accessed by Kopia by viewing Kopia's debug logs (`kopia ... --log-level=debug`); look for lines containing `STORAGE`. Note that `q` blobs are very aggressively cached locally by Kopia, so they may appear to not be accessed when performing basic operations like listing snapshots.

#### Using Archive Storage With Delayed Access

Kopia does **not** currently support cloud storage that provides *delayed* access to your files -- namely, archive storage such as Amazon Glacier Deep Archive. Do not try it; things will break.

If you really want to use archive storage with Kopia, consider using Google Cloud Storage's Archive storage class -- it is more expensive than Amazon Glacier Deep Archive but still very cheap to store ($0.0012 per GB at the time of this writing) and provides instant access to your files; but remember that, like other archive storage, costs are high for accessing files in Google Cloud Storage's Archive storage class.
