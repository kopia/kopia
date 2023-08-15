
---
title: "What is Kopia?"
linkTitle: "What is Kopia?"
weight: 20
---

Kopia is a fast and secure open-source backup/restore tool that allows you to create [encrypted](features/#end-to-end-zero-knowledge-encryption) snapshots of your data and save the snapshots to [remote or cloud storage](features/#save-snapshots-to-cloud-network-or-local-storage) of your choice, [to network-attached storage or server](features/#save-snapshots-to-cloud-network-or-local-storage), or [locally on your machine](features/#save-snapshots-to-cloud-network-or-local-storage). Kopia does not 'image' your whole machine. Rather, Kopia allows you to backup/restore any and all files/directories that you deem are important or critical.

Kopia has both [CLI (command-line interface)](features/#both-command-line-and-graphical-user-interfaces) and [GUI (graphical user interface)](features/#both-command-line-and-graphical-user-interfaces) versions, making it the perfect tool for both advanced and regular users. You can read more about Kopia's unique [features](features/) -- which include [compression](features/#compression), [deduplication](features/#backup-files-and-directories-using-snapshots), [end-to-end 'zero knowledge' encryption](features/#end-to-end-zero-knowledge-encryption), and [error correction](features/#error-correction) -- to get a better understanding of how Kopia works.

When ready, head to the [installation](installation/) page to download and install Kopia, and make sure to read the [Getting Started Guide](getting-started/) for a step-by-step walkthrough of how to use Kopia.

### Pick the Cloud Storage Provider You Want

Kopia supports saving your [encrypted](features/#end-to-end-zero-knowledge-encryption) and [compressed](features/#compression) snapshots to all of the following [storage locations](features/#save-snapshots-to-cloud-network-or-local-storage):

* **Amazon S3** and any **cloud storage that is compatible with S3**
* **Azure Blob Storage**
* **Backblaze B2**
* **Google Cloud Storage**
* Any remote server or cloud storage that supports **WebDAV**
* Any remote server or cloud storage that supports **SFTP**
* Some of the cloud storages supported by **Rclone**
  * Requires you to download and setup Rclone in addition to Kopia, but after that Kopia manages/runs Rclone for you
  * Rclone support is experimental: not all the cloud storages supported by Rclone have been tested to work with Kopia, and some may not work with Kopia; Kopia has been tested to work with **Dropbox**, **OneDrive**, and **Google Drive** through Rclone
* Your local machine and any network-attached storage or server
* Your own server by setting up a [Kopia Repository Server](repository-server/)

And Kopia uses [data deduplication](features/#backup-files-and-directories-using-snapshots) to save you money! Read the [repositories help page](repositories/) for more information on supported storage locations.

With Kopia you are in full control of where to store your snapshots, that is, you pick the storage provider you want to use. You must provision and pay for the storage provider for whatever storage locations you want to use, and then tell Kopia what those storage locations are. You can even use multiple storage locations for different backup repositories if you want. Kopia also supports backing up multiple machines to the same storage location.
