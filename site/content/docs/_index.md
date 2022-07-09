
---
title: "What is Kopia?"
linkTitle: "Documentation"
weight: 20
---

Kopia is a fast and secure open-source backup/restore tool that allows you to create [encrypted](features/#end-to-end-zero-knowledge-encryption) snapshots of your data and save the snapshots to [remote or cloud storage](features/#save-snapshots-to-cloud-network-or-local-storage) of your choice, [to network-attached storage or server](features/#save-snapshots-to-cloud-network-or-local-storage), or [locally on your machine](features/#save-snapshots-to-cloud-network-or-local-storage). Kopia does not 'image' your whole machine. Rather, Kopia allows you to backup/restore any and all files/directories that you deem are important or critical.

Kopia has both [CLI (command-line interface)](features/#both-command-line-and-graphical-user-interfaces) and [GUI (graphical user interface)](features/#both-command-line-and-graphical-user-interfaces) versions, making it the perfect tool for both advanced and regular users. You can read more about Kopia's unique [features](features/) -- which include [compression](features/#compression), [deduplication](features/#backup-files-and-directories-using-snapshots), and [end-to-end 'zero knowledge' encryption](features/#end-to-end-zero-knowledge-encryption) -- to get a better understanding of how Kopia works.

When ready, head to the [installation](installation/) page to download and install Kopia, and make sure to read the [Getting Started Guide](getting-started/) for a step-by-step walkthrough of how to use Kopia.

### Pick the Cloud Storage Provider You Want

Kopia supports saving your [encrypted](features/#end-to-end-zero-knowledge-encryption) and [compressed](features/#compression) snapshots to all of the following [cloud storage](features/#save-snapshots-to-cloud-network-or-local-storage):

* Amazon S3 and any cloud storage that is compatible with S3
  * Including Alibaba Cloud, Amazon Lightsail, Backblaze B2, China Mobile Cloud, Cloudflare R2, Contabo, DigitalOcean Spaces, Dreamhost, Google Cloud Storage, IBM Cloud, IDrive E2, Linode, Mail.ru Cloud, MEGA.io S4, MinIO, Oracle Cloud Infrastructure, OVH, Scaleway, Storj, Synology C2, Tencent Cloud, Vultr, Wasabi, Yandex Cloud, and many more!
* Azure Blob Storage
* Backblaze B2
* Google Cloud Storage
* Any remote server or cloud storage that supports WebDAV
* Any remote server or cloud storage that supports SFTP
* Dropbox, OneDrive, Google Drive, and all cloud storage supported by Rclone
  * Requires you to download and setup Rclone in addition to Kopia, but after that Kopia manages/runs Rclone for you
  * Rclone support is experimental and all the cloud storage supported by Rclone has not been tested to work with Kopia; Kopia has been tested to work with Dropbox, OneDrive, and Google Drive through Rclone
* Your local machine and any network-attached storage or server
* Your own server by setting up a [Kopia Repository Server](https://kopia.io/docs/repository-server/)

And Kopia uses [data deduplication](https://kopia.io/docs/features/#backup-files-and-directories-using-snapshots) to save you money! Read the [repositories help page](../repositories/) for more information on supported storage locations.

With Kopia you’re in full control of where to store your snapshots; you pick the cloud storage you want to use. Kopia plays no role in selecting your storage locations. You must provision and pay (the storage provider) for whatever storage locations you want to use, and then tell Kopia what those storage locations are. The advantage of decoupling the software (i.e., Kopia) from storage is that you can use whatever storage locations you desire -– it makes no difference to Kopia what storage you use. You can even use multiple storage locations if you want to, and Kopia also supports backing up multiple machines to the same storage location.
