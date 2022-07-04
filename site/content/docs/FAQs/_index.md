---
title: "Frequently Asked Questions"
linkTitle: "Frequently Asked Questions"
weight: 4
---

> NOTE: This page is under development. Feel free to contribute as you see fit.

**Is your question not answered here? Please ask in the [Kopia discussion forums](https://kopia.discourse.group/) for help!**

##### What is a Snapshot?

A `snapshot` is a [point-in-time backup](../features#backup-files-and-directories-using-snapshots) of your files/directories; each snapshot contains the files/directories that you can [restore when you need to](../features#restore-snapshots-using-multiple-methods).

##### What is a Repository?

A `repository` is the storage location where your snapshots are saved; Kopia supports [cloud/remote, network, and local storage locations](../features#save-snapshots-to-cloud-network-or-local-storage) and all repositories are [encrypted](../features/#end-to-end-zero-knowledge-encryption) with a passphrase that you designate.

See the [repository help docs](../repository) for more information.

##### What is a Policy?

A `policy` is a set of rules that tells Kopia how to create/manage snapshots; this includes features such as [compression, snapshot retention, and scheduling when to take automatically snapshots](../features#policies-control-what-and-how-filesdirectories-are-saved-in-snapshots).

##### How to Restore Files/Directories?

Files/directories are restored from the snapshots you create. To restore the data you backed up in a snapshot, Kopia gives you three options: 

* mount the contents of a snapshot as a local disk so that you can browse and copy files/directories from the snapshot as if the snapshot is a local directory on your machine;

* restore all files/directories contained in a snapshot to any local or network location that you designate;

* or selectively restore individual files from a snapshot.

The [Getting Started Guide](../getting-started/) provides directions on how to restore files/directions [when using Kopia GUI](../getting-started/#restoring-filesdirectories-from-snapshots) and [when using Kopia CLI](../getting-started/#mounting-snapshots-and-restoring-filesdirectories-from-snapshots).

##### What is a Kopia Repository Server?

See the [Kopia Repository Server help docs](../repository-server) for more information.
