---
title: "Ransomware Protection"
linkTitle: "Ransomware Protection"
weight: 55
---

Some cloud storage providers provide capabilities targeted at protecting against Ransomware attacks. Kopia can be configured to take advantage of those capabilities to provide additional protection for your data.

### What is Ransomware and why do we need extra protections?

For the context of Kopia protection, Ransomware refers to viruses, trojans or other malware that infects a system, and blocks user access to it (often by overwriting files with an encrypted version of themselves), usually requiring to pay for the decryption key to restore access. Because Ransomware often targets 'high-value' user data, and often overwrites files in place, an automated backup solution (for instance running Kopia on a nightly cron-job) can cause the overwritten files to be propagated to cloud storage. For the case just described, having multiple snapshots would appear to be sufficient to restore your data from the cloud, however Ransomware attacks are getting more sophisticated.  A Ransomware application may look for your cloud provider access keys while examining your files, and use that to permanently delete your snapshots on the cloud!  We need to better protect our cloud storage to ensure our snapshots are safe

### Some notes about storage providers

 * Kopia's AWS S3 storage engine supports using both both restricted access keys as well as object-locks for ransomware protection.
    * Many storage providers with S3 compatibility (for instance Backblaze B2) can take full advantage of Kopia's ransomware
      protection when used with the S3 storage engine.
    * Note that not all S3 compatible providers provide sufficient access controls to take advantage of these features.  Notably,
      Google Cloud Storage (GCS) (see below).
 * Kopia's Backblaze B2 storage engine provides aupport for using restricted access keys, but not for object locks at the current time.
    * To use storage locks with Backblaze B2, use the S3 storage engine
 * Kopia's Google Clud Services (GCS) engine provides neither restricted access key nor object-lock support.
    * Google's S3 compatibility layer does not provide sufficient access controls to use these features, and thus Kopia cannot use
      the ransomware mitigation discussed on this page with GCS at this time.

### Using application keys to protect your data

Some cloud storage solutions provide the ability to generate restricted access keys.  These keys can be configured to only allow access to specific data (a specific bucket or path within a bucket), as well as to restrict what capabilities that key has access to.  The easiest solution is to generate a key without any `delete` permissions, and to configure Kopia to use that key.  Now if a Ransomware application finds your key, it can no longer permanently delete any data from the cloud! This might imply that now Kopia can no longer delete old snapshots as art of its maintenance cycle, but that is not the case. When Kopia deletes data from a compatible provider, it is actually replacing the data with a special file that has a `hidden` marker.  This makes the file appear to be deleted, but it can still be accessed by using an older version of the file).  Typically the cloud provider offers 'Lifecycle-Management' to apply a true-deletion of hidden data after a certain period of time.  Since this is an automated process executed by the cloud provider, no 'true delete' is ever executed by Kopia.  As long as the hidden-to-delete time delay is long enough for you to notice the Ransomware, you can still restore the old versions of your data.  Enabling restricted keys does not require any changes in your Kopia workflow, since Kopia does not need to change its behavior at all.  The cost of this is that data will remain on the cloud provider for extra time before being deleted, potentially incurring additional storage charges.

### How to configure restricted access keys

#### AWS

 * `FIXME`: Create a new application key
   * provide access only to the bucket used by Kopia
   * The following capabilities are needed: Read, Write, ListBuckets, ...
 * `FIXME`: Enable Lifecycle management for the Kopia bucket setting the 'Expiration' action to the time you want to ensure your data is protected for

#### Backblaze
  * Backblaze does not allow creation of an application key with restricted permission from the website.  Instead you must use the cli-application to generate a restricted key
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

So far we have secured your access such that even if a bad-actor gets access to your Kopia configuration, they can't do irreparable harm to your cloud backup.  However, what if they get access to your login credentials?  Your login credentials provide the ability to delete your data and even your entire buckets for all of the buckets in your account.  But the cloud providers have protection from that too.  AWS (and S3 compatible providers) offer a feature called 'Object Locking'.  An Object Lock is applied when a file is created (or on an existing file), and it provides a specific retention date.  Until that retention date occurs, there is no way to delete the locked file.  Even using your login credentials, the file is protected from deletion.  It can still be overwritten with a new version, or hidden such that it doesn't appear in a file list.  But it will always be accessible until its retention date occurs.  While Kopia supports applying Object Locks, there are some caveats:

  * Object Locks must be enabled when a bucket is created.  If you already have backups in the cloud, you will need to create a new bucket with Object Locks turned on.
  * Kopia does not currently renew Object Lock retention dates.  This means that if your data doesn't change, eventually the original Retention date will pass, and your data will no longer be protected by an Object Lock even if you are taking regular snapshots.
    * However, this can be mitigated by using an external application to update the Object Locks before they expire.
  * Kopia currently only supports object locks when using an S3 repo.  If you use Backblaze, you will need to use the S3 repo method with Kopia instead of the Backblaze native API.

### How to enable Object Locks

  * Create a new bucket with Object Locks enabled
    * There is no need to set a default Retention time
  * Optional: Create a restricted access key as instructed above for additional protection
  * Create a new Kopia repository
    * Run: `kopia repo create s3 --bucket <bucket name> --access-key=<access key> --secret-access-key=<secret access key> --retention-mode COMPLIANCE --retention-period <retention time>`
      * `retention-period`` can be specified in days (for instance 30d)
  * After each backup (or at least before the retirement period is up) it is important to reset the Object Lock retention time on all current files.
    * Kopia does not currently provide his capability, but the process is to:
      * List all filenames in the bucket (assuming a single repo per bucket or at least consistent retention times)
      * For each found file starting with 'x', 'q', or 'p': apply a PutObjectRetention command with the updated retention time.
        * files starting with _log probably do not need additional retention applied
      * This can be achieved using a reasonably simple python script `TBD`

### How to restore a snapshot that was deleted by Ransomware (or some other process)
  * If you have data that needs to be restored, make sure that either your retention time will not expire or your lifecycle data expiration is sufficient to ensure you can download your data before the cloud provider could remove it
  * Disconnect the repo in Kopia
  * Reconnect the repo in Kopia using the `--point-in-time` option (ex: --point-in-time=2021-11-29T01:10:00.000Z`)
    * This option is only currently available when using an 's3' repo

### A caveat about Ransomware protection

Ransomware can be very sophisticated software.  It may run on your system for weeks, slowly encrypting data that is not frequently used, and only hitting actively used data last (to avoid detection for as long as possible).  This means that you should ensure your data retention period is long enough that you will be able to recover your data.  It also means that recovery may not be easy.  You may have many snapshots after the Ransomware has started modifying your system, and you may have made changes throughout that time such that there is no single snapshot that represents a good state of all of your files.  Restoration is likely to be a time-consuming task, and your best bet is to protect your system from these threats so they don't occur in the first place.  But if they do, Kopia can provide some additional security to protect your data.

Additionally note that ransomware could theoretically weaponize object-locks to cost you a lot of money.  Becuase object-locks cannot be shortened are removed, Sufficiently malicious ransomware could upload large amounts of data and set a very long object lock that would make it impossible to delete.  It is strongly recommended to ensure you have appropriate quotas/limits on your buckets to limit potential storage costs.
