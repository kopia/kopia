Kopia
=====

[![Linux/macOS Build Status](https://travis-ci.org/kopia/kopia.svg?branch=master)](https://travis-ci.org/kopia/kopia)
[![GoDoc](https://godoc.org/github.com/kopia/kopia?status.svg)](https://godoc.org/github.com/kopia/kopia)

> _n. thing exactly replicated from the original (Polish)_

Kopia is a simple, cross-platform tool for managing encrypted backups in the cloud. It provides fast, incremental backups, secure, client-side encryption and data deduplication.

Unlike other cloud backup solutions, the user in full control of backup storage and is responsible for purchasing one of the cloud storage products  (such as [Google Cloud Storage](https://cloud.google.com/storage/)), which offer great durability and availability for your data.

> **NOTICE**:
>
> Kopia is still in early stages of development and is **not ready for general use**.
> The repository and vault data format are subject to change, including backwards-incompatible changes. Use at your own risk.

Installation
---

To build Kopia you need the latest version of [Go](https://golang.org/dl/) and run the following commands:

```
mkdir $HOME/kopia
export GOPATH=$HOME/kopia
go get github.com/kopia/kopia/cmd/kopia
```

This will download and compile Kopia and place the binary in `$HOME/kopia/bin/kopia`. For convenience it's best to place it in a directory it the system `PATH`.

Setting up repository and vault
---

Repository is where the bulk of the backup data will be stored and which can be shared among users. Vault is a small, password-protected area that stores list of backups of a single user.

Vault and repository can be stored on:

- local filesystem paths
- Google Cloud Storage buckets, for example `gs://my-bucket`

For example, to create a vault on a local USB drive and repository in Google Cloud Storage use:

```
$ kopia create --vault /mnt/my-usb-drive --repository gs://my-bucket
Enter password to create new vault: ***********
Re-enter password for verification: ***********
Connected to vault: /mnt/my-usb-drive
```

The vault password is cached in a local file, so you don't need to enter it all the time.
To disconnect from the vault and remove cached password use:
```
$ kopia disconnect
Disconnected from vault.
```

To connect to an existing vault:
```
$ kopia connect --vault /mnt/my-usb-drive
Enter password to open vault: ***********
Connected to vault: /mnt/my-usb-drive
```

Backup and Restore
---

To create a backup of a directory or file, use `kopia backup <path>`. It will print the identifier of a backup, which is a long string, that can be used to restore the file later. Because data in a repository is content-addressable, two files with identical contents, even in different directories or on different machines, will get the same backup identifier.

```
$ kopia backup /path/to/dir
D9691a95c5f9a73a1decf493f8f0d79.309a95f17bc3c6d3272bd0a62d2
$ kopia backup /path/to/dir
D9691a95c5f9a73a1decf493f8f0d79.309a95f17bc3c6d3272bd0a62d2
```

To list all backups of a particular directory or file, use:
```
$ kopia backups <path>
```

To list all backups stored in a vault, use:
```
$ kopia backups --all
```

In order to browse the contents of a backup you can mount it in a local filesystem using:

```
$ kopia mount <backup-identifier> <mount-path>
```

To unmount, use:
```
$ umount <mount-path>
```

You can also show the contents of an single object in a repository by using:
```
$ kopia show <backup-identifier>
```

Licensing
---
Kopia is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full license text.

Disclaimer
---

Kopia is a personal project and is not affiliated with, supported or endorsed by Google.

Cryptography Notice
---

  This distribution includes cryptographic software. The country in
  which you currently reside may have restrictions on the import,
  possession, use, and/or re-export to another country, of encryption
  software. BEFORE using any encryption software, please check your
  country's laws, regulations and policies concerning the import,
  possession, or use, and re-export of encryption software, to see if
  this is permitted. See <http://www.wassenaar.org/> for more
  information.

  The U.S. Government Department of Commerce, Bureau of Industry and
  Security (BIS), has classified this software as Export Commodity
  Control Number (ECCN) 5D002.C.1, which includes information security
  software using or performing cryptographic functions with symmetric
  algorithms. The form and manner of this distribution makes it
  eligible for export under the License Exception ENC Technology
  Software Unrestricted (TSU) exception (see the BIS Export
  Administration Regulations, Section 740.13) for both object code and
  source code.
