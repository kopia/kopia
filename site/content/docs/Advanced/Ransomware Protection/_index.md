---
title: "Ransomware Protection"
linkTitle: "Ransomware Protection"
weight: 55
aliases:
- ../advanced/ransomware/
---

Some cloud storage providers provide capabilities targeted at protecting against ransomware attacks. Kopia can be configured to take advantage of those capabilities to provide additional protection for your data.

### What is ransomware and why do we need extra protections?

For the context of Kopia protection, ransomware refers to viruses, trojans or other malware that infects a system, and blocks user access to it (often by overwriting files with an encrypted version of themselves), usually requiring to pay for the decryption key to restore access. Because ransomware often targets 'high-value' user data, and often overwrites files in place, an automated backup solution (for instance running Kopia on a nightly cron-job) can cause the overwritten files to be propagated to cloud storage. For the case just described, having multiple snapshots would appear to be sufficient to restore your data from the cloud, however ransomware attacks are getting more sophisticated.  A ransomware application may look for your cloud provider access keys while examining your files and use that to permanently delete your snapshots on the cloud!  We need to better protect our cloud storage to ensure our snapshots are safe.

### Some notes about storage providers

 * Kopia's AWS S3 storage engine supports using both restricted access keys as well as object-locks for ransomware protection.
    * Many storage providers with S3 compatibility (for instance Backblaze B2) can take full advantage of Kopia's ransomware
      protection when used with the S3 storage engine.
    * Note that not all S3 compatible providers provide sufficient access controls to take advantage of these features.  Notably,
      Google Cloud Storage (GCS) (see below).
 * Kopia's Backblaze B2 storage engine provides support for using restricted access keys, but not for object locks at the current time.
    * To use storage locks with Backblaze B2, use the S3 storage engine.
* Kopia's Azure & Google storage engines support object-locks for ransomware protection.

### Using application keys to protect your data

Some cloud storage solutions provide the ability to generate restricted access keys.  These keys can be configured to only allow access to specific data (a specific bucket or path within a bucket), as well as to restrict what capabilities that key has access to.  The easiest solution is to generate a key without any `delete` permissions, and to configure Kopia to use that key.  Now if a ransomware application finds your key, it can no longer permanently delete any data from the cloud! This might imply that now Kopia can no longer delete old snapshots as part of its maintenance cycle, but that is not the case. When Kopia deletes data from a compatible provider, it is replacing the data with a special file that has a `hidden` marker.  This makes the file appear to be deleted, but it can still be accessed by using an older version of the file).  Typically, the cloud provider offers 'Lifecycle-Management' to apply a true-deletion of hidden data after a certain period of time.  Since this is an automated process executed by the cloud provider, no 'true delete' is ever executed by Kopia.  As long as the hidden-to-delete time delay is long enough for you to notice the ransomware, you can still restore the old versions of your data.  Enabling restricted keys does not require any changes in your Kopia workflow, since Kopia does not need to change its behavior at all.  The cost of this is that data will remain on the cloud provider for extra time before being deleted, potentially incurring additional storage charges.

### How to configure restricted access keys

#### AWS

 * Create a new application key
   * Create a IAM user for kopia to use
     * Select 'Attach policies directly'
     * Create a new policy, with the following permissions (paste into JSON form)
     ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "Stmt1480692207000",
                    "Effect": "Deny",
                    "Action": [
                        "s3:DeleteBucket",
                        "s3:DeleteBucketPolicy",
                        "s3:DeleteBucketWebsite",
                        "s3:DeleteObjectVersion"
                    ],
                    "Resource": [
                        "arn:aws:s3:::*"
                    ]
                }
            ]
        }
      ```
     * Attach created policy to new user
   * Manage user's security-credentials and create a new access key
 * Disconnect and reconnect your existing Kopia repo using the new key (or create a new bucket using this key)
 * Regenerate (or delete) your root application key if you have one
 * Enable Lifecycle management for the Kopia bucket setting the 'Expiration' action to the time you want to ensure your data is protected for

#### Backblaze
  * Backblaze does not allow creation of an application key with restricted permission from the website.  Instead, you must use the cli-application to generate a restricted key.
    * Download the appropriate CLI application from [Backblaze](https://www.backblaze.com/b2/docs/quick_command_line.html)
    * Generate a new master API key on the [website](https://secure.backblaze.com/app_keys.htm)
    * Set the following environment variables from your Master API key
      * B2_APPLICATION_KEY_ID=xxxxxxxxxxxxxxxxxxxxxxxxx
      * B2_APPLICATION_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    * Run: `b2 create-key --bucket <bucket-name> <key-name> listBuckets,readBuckets,listFiles,readFiles,writeFiles,readBucketEncryption,readBucketReplications,readBucketRetentions,readFileRetentions,writeFileRetentions,readFileLegalHolds`
    * The resulting string contains the restricted application key and password...you will need this for Kopia
    * Disconnect and reconnect your existing Kopia repo using the new key (or create a new bucket using this key)
    * Re-generate your API master API key (to deactivate the one you just used)
    * Delete any existing Application keys that do not have restricted access

### Even more protection

So far, we have secured your access such that even if a bad actor gets access to your Kopia configuration, they can't do irreparable harm to your cloud backup.  However, what if they get access to your login credentials?  Your login credentials provide the ability to delete your data and even your entire buckets for all the buckets in your account.  But the cloud providers have protection from that too.

Multi-factor-authentication (MFA) is one option.  With MFA enabled, an attacker would need access to your password as well as your security device to be able to manipulate your account.  All major providers support MFA, and it is recommended to use it to secure your account.  Note that it is important to eliminate and root/global acess keys as well, since they can generally be used to execute nearly any task you can do when logged in (effectively bypassing MFA).

An additional layer of protection is `Object Locking' that can be enabled in AWS (and S3 compatible providers).  An Object Lock is applied when a file is created (or on an existing file), and it provides a specific retention date.  Until that retention date occurs, there is no way to delete the locked file.  Even using your login credentials, the file is protected from deletion.  It can still be overwritten with a new version or hidden such that it doesn't appear in a file list.  But it will always be accessible until its retention date occurs.  While Kopia supports applying Object Locks, there are some caveats:

  * Object Locks must be enabled when a bucket is created.  If you already have backups in the cloud, you will need to create a new bucket with Object Locks turned on (NOTE: On Backblaze S3, object-lock can be enabled on existing buckets via `b2 update bucket`).  Once a bucket has Object Lock enabled, it cannot be disabled.
  * You must enable Object Lock extension in Kopia.  By default, Kopia does not renew Object Lock retention dates, however this can be enabled in the `full-maintenance` options.  You must ensure that you run full maintenance at least as frequently as your Object Lock period to ensure Object Locks do not expire, otherwise the Retention date will pass, and your data will no longer be protected by an Object Locks.
  * It is strongly recommended to use compliance mode when creating the Kopia repository.  Compliance mode ensures that even root users cannot delete files from a bucket, and provides the highest level of security.  More information can be found in the [S3 documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-overview.html#object-lock-retention-modes)
  * Kopia currently only supports object locks when using an S3 repo.  If you use Backblaze, you will need to use the S3 repo method with Kopia instead of the Backblaze native API.

### How to enable Object Locks

  * Create a new bucket with Object Locks enabled
    * There is no need to set a default Retention time
    * Backblaze S3 uses can use `b2 update bucket` to enable Object Locks on an existing bucket
  * Optional: Create a restricted access key as instructed above for additional protection
  * Create a new Kopia repository
    * Run: `kopia repo create s3 --bucket <bucket name> --access-key=<access key> --secret-access-key=<secret access key> --retention-mode COMPLIANCE --retention-period <retention time>`
      * `retention-period` can be specified in days (for instance 30d)
  * Enable Object Lock extension
    * Run: `kopia maintenance set --extend-object-locks true`
      * Note that the `full-interval` must be at least 1 day shorter than the `retention-period` or Kopia will not allow you to enable Object Lock extension

### How to restore a snapshot that was deleted by ransomware (or some other process)
  * If you have data that needs to be restored, make sure that either your retention time will not expire or your lifecycle data expiration is sufficient to ensure you can download your data before the cloud provider removes it
  * Disconnect the repo in Kopia
  * Reconnect the repo in Kopia using the `--point-in-time` option (ex: `--point-in-time=2021-11-29T01:10:00.000Z`)
    * This option is only currently available when using an 's3' repo

### A caveat about ransomware protection

Ransomware can be very sophisticated software.  It may run on your system for weeks, slowly encrypting data that is not frequently used, and only hitting actively used data last (to avoid detection for as long as possible).  This means that you should ensure your data retention period is long enough that you will be able to recover your data.  It also means that recovery may not be easy.  You may have many snapshots after the ransomware has started modifying your system, and you may have made changes throughout that time such that there is no single snapshot that represents a good state of all your files.  Restoration is likely to be a time-consuming task, and your best bet is to protect your system from these threats, so they don't occur in the first place.  But if they do, Kopia can provide some additional security to protect your data.

Additionally note that ransomware could theoretically weaponize object-locks to cost you a lot of money.  Because object-locks cannot be reduced or removed, sufficiently malicious ransomware could upload large amounts of data and set a very long object lock that would make it impossible to delete.  It is strongly recommended to ensure you have appropriate quotas/limits on your buckets to limit potential storage costs.

### An additional note about Lifecycle Management vs retention-time

At first glance, Lifecycle Management and retention-time may seem to serve similar purposes.  However, if only using Lifecycle Management, an attacker could still log into your account and delete the entire bucket, or otherwise force-delete a file.  Using 'Object Lock' with retention-time provides an additional guarantee that the only way for data to be lost before the retention-time expires would be to delete your account altogether.  The S3 provider may allow enabling Object Lock without enabling Lifecycle Management.  When retention-time is applied to a file, and that file is deleted, the S3 service will set a `DELETE` marker instead of actually deleting the file. If Lifecycle Management is not enabled, then files may remain in the repository with  the `DELETED` tag indefinitely.  Thus, it is recommended to enable Lifecycle Management whenever using a retention-time in Kopia to balance protective measures against escalating storage costs.

For simplicity, the recommendation is to use the same time period for Lifecycle Management and for retention-time, however, this is not a hard requirement.  It is possible to set a very short Lifecycle Management period and a long retention-time (in which case files will be permanently deleted soon after the retention-time expires.  Alternatively, the Lifecycle Management could be set to be significantly longer than the retention time.  This would provide additional restore capabilities while allowing for manual cleanup of deleted files should it be necessary (with the understanding that once the retention-time expires, the ransomware protention is reduced).  For simplicity, the recommendation is to use the same time period for Lifecycle Management and for retention-time.

### Azure protection

Kopia supports ransomware protection for Azure in a similar manner to S3. The container must have version-level immutability support enabled and the related storage account must have versioning enabled.
When this is configured, the retention mode can be set to either compliance or governance mode. In both cases the blobs will be in [Locked](https://learn.microsoft.com/en-us/rest/api/storageservices/set-blob-immutability-policy?tabs=microsoft-entra-id#remarks) mode. 

Follow [these steps](https://learn.microsoft.com/en-us/azure/storage/blobs/versioning-enable) to enable versioning on the storage account and [these steps](https://learn.microsoft.com/en-us/azure/storage/blobs/immutable-policy-configure-version-scope) to enable version-level immutability support on the container or related storage account.

On Kopia side `--retention-mode COMPLIANCE --retention-period <retention time>` should be set like above.

To have continuous protection it is also necessary to run: `kopia maintenance set --extend-object-locks true`
* Note that the `full-interval` must be at least 1 day shorter than the `retention-period` or Kopia will not allow you to enable Object Lock extension

### Google protection

Kopia supports ransomware protection for Google in a similar manner to S3. The bucket must have both versioning and object retention enabled.
When this is configured, the retention mode can be set to either compliance or governance mode. In both cases the blobs will be in [Locked](https://cloud.google.com/storage/docs/object-lock#overview) mode.

On Kopia side `--retention-mode COMPLIANCE --retention-period <retention time>` should be set like above.

To have continuous protection it is also necessary to run: `kopia maintenance set --extend-object-locks true`
* Note that the `full-interval` must be at least 1 day shorter than the `retention-period` or Kopia will not allow you to enable Object Lock extension

If using minimal permissions with the credentials,
`storage.objects.setRetention` permission is also required.
