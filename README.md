Kopia
=====

![Kopia](icons/kopia.svg)
[![Build Status](https://github.com/kopia/kopia/workflows/Build/badge.svg)](https://github.com/kopia/kopia/actions?query=workflow%3ABuild)
[![Slack](https://img.shields.io/badge/discuss-slack-blue.svg)](https://slack.kopia.io/) 
[![GoDoc](https://godoc.org/github.com/kopia/kopia/repo?status.svg)](https://godoc.org/github.com/kopia/kopia/repo)
[![Coverage Status](https://codecov.io/gh/kopia/kopia/branch/master/graph/badge.svg?token=CRK4RMRFSH)](https://codecov.io/gh/kopia/kopia)[![Go Report Card](https://goreportcard.com/badge/github.com/kopia/kopia)](https://goreportcard.com/report/github.com/kopia/kopia)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)
[![Docker Pulls](https://img.shields.io/docker/pulls/kopia/kopia)](https://hub.docker.com/r/kopia/kopia/tags?page=1&ordering=name)
[![Downloads](https://img.shields.io/github/downloads/kopia/kopia/total.svg)](https://github.com/kopia/kopia/releases)
[![Gurubase](https://img.shields.io/badge/Gurubase-Ask%20Kopia%20Guru-006BFF)](https://gurubase.io/g/kopia)

> _n._
>
> 1. _[copy, replica](https://en.wikipedia.org/wiki/Replica) (Polish)_
> 2. _[lance, spear](https://en.wikipedia.org/wiki/Kopia)_
> 3. _[fast and secure backup tool](https://kopia.io)_


Kopia is a fast and secure open-source backup/restore tool that allows you to create [encrypted](https://kopia.io/docs/features/#end-to-end-zero-knowledge-encryption) snapshots of your data and save the snapshots to [remote or cloud storage](https://kopia.io/docs/features/#save-snapshots-to-cloud-network-or-local-storage) of your choice, [to network-attached storage or server](https://kopia.io/docs/features/#save-snapshots-to-cloud-network-or-local-storage), or [locally on your machine](https://kopia.io/docs/features/#save-snapshots-to-cloud-network-or-local-storage). Kopia does not 'image' your whole machine. Rather, Kopia allows you to backup/restore any and all files/directories that you deem are important or critical.

Kopia has both [CLI (command-line interface)](https://kopia.io/docs/features/#both-command-line-and-graphical-user-interfaces) and [GUI (graphical user interface)](https://kopia.io/docs/features/#both-command-line-and-graphical-user-interfaces) versions, making it the perfect tool for both advanced and regular users. You can read more about Kopia's unique [features](https://kopia.io/docs/features/) -- which include [compression](https://kopia.io/docs/features/#compression), [deduplication](https://kopia.io/docs/features/#backup-files-and-directories-using-snapshots), [end-to-end 'zero knowledge' encryption](https://kopia.io/docs/features/#end-to-end-zero-knowledge-encryption), and [error correction](https://kopia.io/docs/features/#error-correction) -- to get a better understanding of how Kopia works.

When ready, head to the [installation](https://kopia.io/docs/installation/) page to download and install Kopia, and make sure to read the [Getting Started Guide](https://kopia.io/docs/getting-started/) for a step-by-step walkthrough of how to use Kopia.

Pick the Cloud Storage Provider You Want
---

Kopia supports saving your [encrypted](https://kopia.io/docs/features/#end-to-end-zero-knowledge-encryption) and [compressed](https://kopia.io/docs/features/#compression) snapshots to all of the following [storage locations](https://kopia.io/docs/features/#save-snapshots-to-cloud-network-or-local-storage):

* **Amazon S3** and any **cloud storage that is compatible with S3**
* **Azure Blob Storage**
* **Backblaze B2**
* **Google Cloud Storage**
* Any remote server or cloud storage that supports **WebDAV**
* Any remote server or cloud storage that supports **SFTP**
* Some of the cloud storage options supported by **Rclone**
  * Requires you to download and setup Rclone in addition to Kopia, but after that Kopia manages/runs Rclone for you
  * Rclone support is experimental: not all the cloud storage products supported by Rclone have been tested to work with Kopia, and some may not work with Kopia; Kopia has been tested to work with **Dropbox**, **OneDrive**, and **Google Drive** through Rclone
* Your local machine and any network-attached storage or server
* Your own server by setting up a [Kopia Repository Server](https://kopia.io/docs/repository-server/)

And Kopia uses [data deduplication](https://kopia.io/docs/features/#backup-files-and-directories-using-snapshots) to save you money! Read the [repositories help page](https://kopia.io/docs/repositories/) for more information on supported storage locations.

With Kopia you are in full control of where to store your snapshots, that is, you pick the storage provider you want to use. You must provision and pay for the storage provider for whatever storage locations you want to use, and then tell Kopia what those storage locations are. You can even use multiple storage locations for different backup repositories if you want. Kopia also supports backing up multiple machines to the same storage location.

Kopia in Action
---

Using Kopia via command-line interface:

[![asciicast](https://asciinema.org/a/ykx6uzEhKY3451fWEnX9nm9uo.svg)](https://asciinema.org/a/ykx6uzEhKY3451fWEnX9nm9uo)

Using Kopia via graphical user interface (note: the video is of an older version of Kopia and the interface is different in the current version of Kopia, but the main principles of the interface are the same):

[![Kopia UI Tutorial](https://img.youtube.com/vi/sHJjSpasWIo/0.jpg)](https://www.youtube.com/watch?v=sHJjSpasWIo)

Getting Started
---
See [Kopia Documentation](https://kopia.io/docs/) for more information.

Building Kopia
---
See [Build Infrastructure](BUILD.md) for more information on building Kopia and working with the source code.

Licensing
---
Kopia is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full license text.

Contribution Guidelines
---
Kopia is open source and contributions are welcome. For more information on how to contribute see the [Contribution Guidelines](https://kopia.io/docs/contribution-guidelines/).

Reporting Security Issues
---
If you find a security issue you'd like to disclose privately, please contact `security@kopia.io` or via direct message to maintainers on [Slack](https://slack.kopia.io).

[![Netlify Status](https://api.netlify.com/api/v1/badges/6b5c1fe4-a0da-4e7e-939b-ff1105251985/deploy-status)](https://app.netlify.com/sites/kopia/deploys)
