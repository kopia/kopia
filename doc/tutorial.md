Tutorial
===

Kopia is a simple, cross-platform tool for managing encrypted backups in the cloud. It provides fast incremental backups, data deduplication and client-side encryption.

The main difference from other backup solutions is that as a user, you are in full control of backup storage - in fact you are responsible for purchasing one of the cloud storage products available (such as Google Cloud Storage), which offer great durability and availability for your data.

## Installation

### Binary Releases

You can download pre-built `kopia` binary from http://kopia.github.io/download. Once downloaded, it's best to put it in a directory that's in system PATH, such as `/usr/local/bin`.

### Installation From Source

To build Kopia from source you need the latest version of [Go](https://golang.org/dl/) and run the following commands:

```
mkdir $HOME/kopia
export GOPATH=$HOME/kopia
go get github.com/kopia/kopia
go install github.com/kopia/kopia/cmd/kopia
```

This will automatically download and build kopia and put the resulting binary in `$HOME/kopia/bin`. For convenience it's best to add this directory to system `PATH` or copy/symlink it to a directory already in the path, such as `/usr/local/bin`.

## Getting Started

To use Kopia, you need to set up two storage locations:

- **Repository** - which will store the bulk of encrypted data
- **Vault** - which stores the backup metadata and their encryption keys

The **Repository** can be shared between multiple computers or users because without the encryption keys stored in the **Vault** its data is unaccessible.

The metadata stored in the *Vault* is very small and will typically fit on even smallest of USB drives for safekeeping. You have a choice of using Vault in the cloud or keeping it on a removable storage device in your possession.

### Setting Up Vault

To create new vault in Google Cloud Storage, first create the bucket bucket using https://console.cloud.google.com/storage/ then run:

```
kopia vault create gcs BUCKET
```

To create new vault in the local filesystem, use:

```
mkdir /path/to/vault
kopia vault create filesystem /path/to/vault
```

You will be prompted for the password to protect the data in the vault. **Don't forget your password, as there is absolutely no way to recover data in the vault if you do so.**

To later connect to an existing vault, simply replace `vault create` with `vault connect`.

To disconnect from a vault, run:
```
kopia vault disconnect
```

### Setting Up Repository

After creating the vault, we now need to create a repository. This is very similar way to creating a vault, just replace `vault` with `repository`. For example to create GCS-backed repository, use:

```
kopia repository create gcs BUCKET
```



