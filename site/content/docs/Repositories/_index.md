---
title: "Repositories"
linkTitle: "Supported Storage Locations"
weight: 25
---

Kopia allows you to save your [encrypted](../features/#end-to-end-zero-knowledge-encryption) backups (which are called [`snapshots`](../faqs/#what-is-a-snapshot) in Kopia) to a variety of storage locations, and in Kopia a storage location is called a `repository`. Kopia supports all of the following storage locations:

> PRO TIP: You pick the storage locations you want to use. Kopia plays no role in selecting your storage locations. This means you must provision, setup, and pay (the storage provider) for whatever storage locations you want to use **before** you create a `repository` for that storage location in Kopia.

* [Amazon S3 and S3-compatible Cloud Storage](#amazon-s3-and-s3-compatible-cloud-storage)
  * Kopia supports all cloud storage platforms that support the S3 API
  * Kopia supports object locking and [hot, cold, and archive storage classes](../advanced/amazon-s3/) for any cloud storage that supports the features using the S3 API
* [Azure Blob Storage](#azure-blob-storage)
* [Backblaze B2](#backblaze-b2)
* [Google Cloud Storage](#google-cloud-storage)
* [Google Drive](#google-drive)
  * Kopia supports Google Drive natively and through Kopia's Rclone option (see below)
  * Native support for Google Drive in Kopia is currently experimental
  * Native Google Drive support operates differently than Kopia's support for Google Drive through Rclone; you will not be able to use the two interchangeably, so pick one
* All remote servers or cloud storage that support [WebDAV](#webdav) 
* All remote servers or cloud storage that support [SFTP](#sftp)
* Some of the cloud storages supported by [Rclone](#rclone) 
  * Rclone is a (free and open-source) third-party program that you must download and setup separately before you can use it with Kopia
  * Once you setup Rclone, Kopia automatically manages and runs Rclone for you, so you do not need to do much beyond the initial setup, aside from enabling Rclone's self-update feature so that it stays up-to-date
  * Kopia's Rclone support is experimental: not all the cloud storages supported by Rclone have been tested to work with Kopia, and some may not work with Kopia; Kopia has been tested to work with [Dropbox](#rclone), [OneDrive](#rclone), and [Google Drive](#rclone) through Rclone
* Your local machine and any network-attached storage or server 
* Your own remote server by setting up a [Kopia Repository Server](../repository-server/)

> PRO TIP: Many cloud storage providers offer a variety of [storage tiers](../advanced/storage-tiers/) that may (or may not) help decrease your cost of cloud storage, depending on your use case. See the [storage tiers documentation](../advanced/storage-tiers/) to learn the different types of files Kopia stores in repositories and which one of these file types you can possibly move to archive tiers, such as Amazon Deep Glacier.

## Amazon S3 and S3-compatible Cloud Storage

Creating an Amazon S3 or S3-compatible storage `repository` is done differently depending on if you use Kopia GUI or Kopia CLI.

### Kopia GUI

Select the `Amazon S3 and Compatible Storage` option in the `Repository` tab in `KopiaUI`. Then, follow on-screen instructions.  You will need to enter `Bucket` name, `Server Endpoint`, `Access Key ID`, and `Secret Access Key`. You can optionally enter an `Override Region` and `Session Token`.

> NOTE: Some S3-compatible cloud storage may have slightly different names for bucket, endpoint, access key, secret key, region, and session token. This will vary between cloud storages. Read the help documentation for the cloud storage you are using to find the appropriate values. You can typically find this information by searching for the S3 API settings for your cloud storage.

You will next need to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password! At this same password screen, you have the option to change the `Encryption` algorithm, `Hash` algorithm, `Splitter` algorithm, `Repository Format`, `Username`, and `Hostname`. Click the `Show Advanced Options` button to access these settings. If you do not understand what these settings are, do not change them because the default settings are the best settings.

> NOTE: Some settings, such as object locking and [actions](../advanced/actions/), can only be enabled when you create a new `repository` using command-line (see next section). However, once you create the `repository` via command-line, you can use the `repository` as normal in Kopia GUI: just connect to the `repository` as described above after you have created it in command-line.

Once you do all that, your repository should be created and you can start backing up your data!

### Kopia CLI

#### Creating a Repository

You must use the [`kopia repository create s3` command](../reference/command-line/common/repository-create-s3/) to create a `repository`:

```shell
$ kopia repository create s3 \
        --bucket=... \
        --access-key=... \
        --secret-access-key=...
```

At a minimum, you will need to enter the bucket name, access key, and secret access key. If you are not using Amazon S3 and are using an S3-compatible storage, you will also need to enter the endpoint and may need to enter the `region`. There are also various other options (such as object locking and [actions](../advanced/actions/)) you can change or enable -- see the [help docs](../reference/command-line/common/repository-create-s3/) for more information.

You will be asked to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password!

#### Connecting to Repository

After you have created the `repository`, you connect to it using the [`kopia repository connect s3` command](../reference/command-line/common/repository-connect-s3/). Read the [help docs](../reference/command-line/common/repository-connect-s3/) for more information on the options available for this command.

## Azure Blob Storage

Creating an Azure Blob Storage `repository` is done differently depending on if you use Kopia GUI or Kopia CLI.

### Kopia GUI

Select the `Azure Blob Storage` option in the `Repository` tab in `KopiaUI`. Then, follow on-screen instructions.  You will need to enter `Container` name, `Storage Account` name and either `Access Key` or `SAS Token`. You can optionally enter an `Azure Storage Domain`.

You will next need to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password! At this same password screen, you have the option to change the `Encryption` algorithm, `Hash` algorithm, `Splitter` algorithm, `Repository Format`, `Username`, and `Hostname`. Click the `Show Advanced Options` button to access these settings. If you do not understand what these settings are, do not change them because the default settings are the best settings.

> NOTE: Some settings, such as [actions](../advanced/actions/), can only be enabled when you create a new `repository` using command-line (see next section). However, once you create the `repository` via command-line, you can use the `repository` as normal in Kopia GUI: just connect to the `repository` as described above after you have created it in command-line.

Once you do all that, your repository should be created and you can start backing up your data!

### Kopia CLI

#### Creating a Repository

You must use the [`kopia repository create azure` command](../reference/command-line/common/repository-create-azure/) to create a `repository`:

```shell
$ kopia repository create azure \
        --container=... \
        --storage-account=... \
        --storage-key=...
```

OR

```shell
$ kopia repository create azure \
        --container=... \
        --storage-account=... \
        --sas-token=...
```

At a minimum, you will need to enter the container name, storage account name, and either your Azure account access key/storage key or a SAS token. There are also various other options (such as [actions](../advanced/actions/)) you can change or enable -- see the [help docs](../reference/command-line/common/repository-create-azure/) for more information.

You will be asked to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password!

#### Connecting to Repository

After you have created the `repository`, you connect to it using the [`kopia repository connect azure` command](../reference/command-line/common/repository-connect-azure/). Read the [help docs](../reference/command-line/common/repository-connect-azure/) for more information on the options available for this command.

## Backblaze B2

Creating a Backblaze B2 `repository` is done differently depending on if you use Kopia GUI or Kopia CLI.

> NOTE: Currently, object locking is supported for B2 but only through Kopia's [S3-compatible storage `repository`](#amazon-s3-and-s3-compatible-cloud-storage) and not through the B2 `repository` option. However, B2 is fully S3 compatible, so you can setup your B2 account via Kopia's [S3 `repository` option](#amazon-s3-and-s3-compatible-cloud-storage). To use B2 storage with the S3 `repository` option the `--endpoint` argument must be specified with the appropriate B2 endpoint. This endpoint can be found on the buckets page of the B2 web interface and follows the pattern `s3.<region>.backblazeb2.com`.

### Kopia GUI

Select the `Backblaze B2` option in the `Repository` tab in `KopiaUI`. Then, follow on-screen instructions.  You will need to enter `B2 Bucket` name, `Key ID`, and application `Key`.

You will next need to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password! At this same password screen, you have the option to change the `Encryption` algorithm, `Hash` algorithm, `Splitter` algorithm, `Repository Format`, `Username`, and `Hostname`. Click the `Show Advanced Options` button to access these settings. If you do not understand what these settings are, do not change them because the default settings are the best settings.

> NOTE: Some settings, such as [actions](../advanced/actions/), can only be enabled when you create a new `repository` using command-line (see next section). However, once you create the `repository` via command-line, you can use the `repository` as normal in Kopia GUI: just connect to the `repository` as described above after you have created it in command-line.

Once you do all that, your repository should be created and you can start backing up your data!

### Kopia CLI

#### Creating a Repository

You must use the [`kopia repository create b2` command](../reference/command-line/common/repository-create-b2/) to create a `repository`:

```shell
$ kopia repository create b2 \
        --bucket=... \
        --key-id=... \
        --key=...
```

There are also various other options (such as [actions](../advanced/actions/)) you can change or enable -- see the [help docs](../reference/command-line/common/repository-create-b2/) for more information.

You will be asked to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password!

#### Connecting to Repository

After you have created the `repository`, you connect to it using the [`kopia repository connect b2` command](../reference/command-line/common/repository-connect-b2/). Read the [help docs](../reference/command-line/common/repository-connect-b2/) for more information on the options available for this command.

## Google Cloud Storage

Creating a Google Cloud Storage `repository` is done differently depending on if you use Kopia GUI or Kopia CLI.

### Kopia GUI

Select the `Google Cloud Storage` option in the `Repository` tab in `KopiaUI`. Then, follow on-screen instructions.  You will need to enter the `GCS Bucket` name and enter the path to where on your machine you have saved the Google Cloud Storage `Credentials File`. The credentials file can be obtained by [creating a Google Cloud Service Account](https://cloud.google.com/docs/authentication/getting-started#create-service-account-console) that allows you to access your storage bucket and then downloading the JSON key file for that service account. You enter the path to this JSON key file in the `Credentials File` textbox in `KopiaUI`.

You will next need to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password! At this same password screen, you have the option to change the `Encryption` algorithm, `Hash` algorithm, `Splitter` algorithm, `Repository Format`, `Username`, and `Hostname`. Click the `Show Advanced Options` button to access these settings. If you do not understand what these settings are, do not change them because the default settings are the best settings.

> NOTE: Some settings, such as [actions](../advanced/actions/), can only be enabled when you create a new `repository` using command-line (see next section). However, once you create the `repository` via command-line, you can use the `repository` as normal in Kopia GUI: just connect to the `repository` as described above after you have created it in command-line.

Once you do all that, your repository should be created and you can start backing up your data!

### Kopia CLI

#### Creating a Repository

There are three methods to create a `repository` for Google Cloud Storage: one that requires you to install Google Cloud SDK; the other method allows you to generate credentials without Google Cloud SDK; and the third method allows you to use Google Cloud Storage through Kopia's [S3 `repository` option](#amazon-s3-and-s3-compatible-cloud-storage):
##### Method #1: Installing Google Cloud SDK

1. Create a storage bucket in [Google Cloud Console](https://console.cloud.google.com/storage/)
2. Install [Google Cloud SDK](https://cloud.google.com/sdk/)
3. Log in with credentials that have permissions to the bucket

```shell
$ gcloud auth application-default login
```

After these preparations, we can create a Kopia `repository` (assuming bucket named `kopia-test-123`) using the [`kopia repository create gcs` command](../reference/command-line/common/repository-connect-gcs/):

```shell
$ kopia repository create gcs --bucket kopia-test-123
```

There are also various other options (such as [actions](../advanced/actions/)) you can change or enable -- see the [help docs](../reference/command-line/common/repository-create-gcs/) for more information.

You will be asked to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password!

##### Method #2: Creating a Service Account and Using the JSON Key File

1. Create a storage bucket in [Google Cloud Console](https://console.cloud.google.com/storage/)
2. Create a Google Cloud Service Account that allows you to access your storage bucket. Directions are available on [Google Cloud's website](https://cloud.google.com/authentication/getting-started#create-service-account-console). Make sure to download the JSON key file for your service account and keep it safe.

After these preparations, we can create a Kopia `repository` (assuming bucket named `kopia-test-123`) using the [`kopia repository create gcs` command](../reference/command-line/common/repository-connect-gcs/):

```shell
$ kopia repository create gcs --credentials-file="/path/to/your/credentials/file.json" --bucket kopia-test-123
```

There are also various other options (such as [actions](../advanced/actions/)) you can change or enable -- see the [help docs](../reference/command-line/common/repository-create-gcs/) for more information.

You will be asked to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password!

##### Method #3: Enabling Amazon S3 Interoperability in Google Cloud Storage

1. Create a storage bucket in [Google Cloud Console](https://console.cloud.google.com/storage/)
2. Go to [Settings and then Interoperability](https://console.cloud.google.com/storage/settings;tab=interoperability) in your Google Cloud Storage account
3. Enable your project under `Default project for interoperable access` and generate access keys for this project -- you will generate both access key and secret key, just like if you were using Amazon S3

After these preparations, we can create a Kopia `repository` (assuming bucket named `kopia-test-123`) using the [`kopia repository create s3` command](../reference/command-line/common/repository-connect-s3/):

```shell
$ kopia repository create s3 --endpoint="storage.googleapis.com" --bucket="kopia-test-123" --access-key="access/key/here" --secret-access-key="secret/key/here"
```

There are also various other options (such as [actions](../advanced/actions/)) you can change or enable -- see the [help docs](../reference/command-line/common/repository-create-s3/) for more information.

You will be asked to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password!

#### Connecting to Repository

After you have created the `repository`, you connect to it using the [`kopia repository connect gcs` command](../reference/command-line/common/repository-connect-gcs/) or the [`kopia repository connect s3` command](../reference/command-line/common/repository-connect-s3/), depending on whichever way you setup the Google Cloud Storage `repository`. Read the [help docs for `repository connect gcs`](../reference/command-line/common/repository-connect-gcs/) or the [help docs for `repository connect s3`](../reference/command-line/common/repository-connect-s3/) for more information on the options available for these commands.

### Credential permissions

The following permissions are required when in readonly mode:
```
storage.buckets.get
storage.objects.get
storage.objects.list
```

When in normal read-write mode the following additional permissions are required:
```
storage.objects.update
storage.objects.create
storage.objects.delete
```

If using [ransomware protection](../advanced/ransomware#Google-protection) then the following additional permission is required:
```
storage.objects.setRetention
```

## Google Drive

Kopia supports Google Drive in two ways: natively and through Kopia's [Rclone `repository` option](#rclone). Native Google Drive support is currently only available through Kopia CLI; Kopia GUI users need to use Kopia's [Rclone `repository` option](#rclone).

Below we describe how to setup a `repository` via Kopia CLI using native Google Drive support, and you will need a Google Cloud Storage service account for the process. You do not need a Google Cloud Storage service account if you create a Google Drive `repository` in Kopia through Rclone; to do that, read the [Rclone section of this page](#rclone). 

> WARNING: Native Google Drive support is experimental.

Kopia uses a Google Drive folder that you provide to store all the files in the `repository`. Kopia will only access files in this folder, and using Kopia does not impact your other Google Drive files. It is recommended that you let Kopia manage this folder and do not upload any other content to this folder.

### Kopia CLI

#### Creating a Repository

Here's a high-level rundown of what you will need to do to create a Google Drive `repository`:

1. Create or use an existing Google Drive folder for the repository.

2. Create a [Google Cloud Service Account](https://cloud.google.com/iam/docs/understanding-service-accounts) for Kopia.

3. Share the Google Drive folder with your new service account so that it allows Kopia to access the folder.

Ready? Here are the step-by-step instructions:

1. [Create a Google Cloud project](https://console.cloud.google.com/projectcreate), or use an existing one.

2. [Enable the Google Drive API](https://console.cloud.google.com/apis/library/drive.googleapis.com) for your project.

3. [Create a service account](https://console.cloud.google.com/iam-admin/serviceaccounts). After enabling the API, you should be now prompted to [create credentials](https://console.cloud.google.com/apis/api/drive.googleapis.com/credentials). Choose `Service account` from the options, and give it a name. Note down the service account email.

4. Create a key for the service account. You can do this by [viewing the service account](https://console.cloud.google.com/iam-admin/serviceaccounts), navigating to the `Keys` tab, and clicking `Add Key` -> `Create new key`. You should choose `JSON` for the key type. Save the file on your computer.

5. Create or pick an existing Google Drive folder. The browser URL should look something like `https://drive.google.com/drive/u/0/folders/[folder_id]`. Note down the last part of the URL. That's your folder ID.

6. Share the Google Drive folder with the service account. Open the share dialog for the folder by right-clicking the folder in Google Drive, and put in the service account email. You should choose the `Editor` as the access role.

After these preparations, we can create a Kopia `repository` (assuming the folder ID is `z63ZZ1Npv3OFvDPwU3dX0w`) using the [`kopia repository create gdrive` command](../reference/command-line/common/repository-connect-gdrive/):

```shell
$ kopia repository create gdrive \
        --folder-id z63ZZ1Npv3OFvDPwU3dX0w \
        --credentials-file="<where-you-have-stored-the-json-key-file>"
```

There are also various other options (such as [actions](../advanced/actions/)) you can change or enable -- see the [help docs](../reference/command-line/common/repository-create-gdrive/) for more information.

You will be asked to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password!

If you view your folder on Google Drive, you should see that Kopia has created the skeleton of the repository with a `kopia.repository` file and a couple of others. Kopia will store all the files for your snapshots in this folder.

### Connecting to Repository

After you have created the `repository`, you connect to it using the [`kopia repository connect gdrive` command](../reference/command-line/common/repository-connect-gdrive/). Read the [help docs](../reference/command-line/common/repository-connect-gdrive/) for more information on the options available for this command.

## WebDAV

Creating a WebDAV `repository` is done differently depending on if you use Kopia GUI or Kopia CLI.

### Kopia GUI

Select the `WebDAV Server` option in the `Repository` tab in `KopiaUI`. Then, follow on-screen instructions.  You will need to enter `WebDAV Server URL`, `Username`, and `Password.

You will next need to enter the repository password that you want. This password can be whatever you want, it does not need to be the same as your WebDAV password. In fact, it should not be the same! Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password! At this same password screen, you have the option to change the `Encryption` algorithm, `Hash` algorithm, `Splitter` algorithm, `Repository Format`, `Username`, and `Hostname`. Click the `Show Advanced Options` button to access these settings. If you do not understand what these settings are, do not change them because the default settings are the best settings.

> NOTE: Some settings, such as [actions](../advanced/actions/), can only be enabled when you create a new `repository` using command-line (see next section). However, once you create the `repository` via command-line, you can use the `repository` as normal in Kopia GUI: just connect to the `repository` as described above after you have created it in command-line.

Once you do all that, your repository should be created and you can start backing up your data!

### Kopia CLI

#### Creating a Repository

You must use the [`kopia repository create webdav` command](../reference/command-line/common/repository-create-webdav/) to create a `repository`:

```shell
$ kopia repository create webdav \
        --url=... \
        --webdav-password=... \
        --webdav-username=...
```


At a minimum, you will need to enter the WebDAV server URL, username, and password. There are also various other options (such as [actions](../advanced/actions/)) you can change or enable -- see the [help docs](../reference/command-line/common/repository-create-webdav/) for more information.

You will be asked to enter the repository password that you want. This password can be whatever you want, it does not need to be the same as your WebDAV password. In fact, it should not be the same! Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password!

#### Connecting to Repository

After you have created the `repository`, you connect to it using the [`kopia repository connect webdav` command](../reference/command-line/common/repository-connect-webdav/). Read the [help docs](../reference/command-line/common/repository-connect-webdav/) for more information on the options available for this command.

## SFTP

Creating a SFTP or SSH `repository` is done differently depending on if you use Kopia GUI or Kopia CLI.

### Kopia GUI

Select the `SFTP Server` option in the `Repository` tab in `KopiaUI`. Then, follow on-screen instructions.  You will need to enter `Host`, `User`, `Path`, and either `Password` or `Path to key file`. You can optionally enter `Path to known_hosts file`.

If the connection to SFTP server does not work, checking the option for `Launch external password-less SSH command` which will launch an external `ssh` process  that supports more connectivity options and may be needed for some hosts.

You will next need to enter the repository password that you want. This password can be whatever you want, it does not need to be the same as your SFTP password. In fact, it should not be the same! Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password! At this same password screen, you have the option to change the `Encryption` algorithm, `Hash` algorithm, `Splitter` algorithm, `Repository Format`, `Username`, and `Hostname`. Click the `Show Advanced Options` button to access these settings. If you do not understand what these settings are, do not change them because the default settings are the best settings.

> NOTE: Some settings, such as [actions](../advanced/actions/), can only be enabled when you create a new `repository` using command-line (see next section). However, once you create the `repository` via command-line, you can use the `repository` as normal in Kopia GUI: just connect to the `repository` as described above after you have created it in command-line.

Once you do all that, your repository should be created and you can start backing up your data!

### Kopia CLI

#### Creating a Repository

You must use the [`kopia repository create sftp` command](../reference/command-line/common/repository-create-sftp/) to create a `repository`:

```shell
$ kopia repository create sftp \
        --path=... \
        --host=... \
        --username=... \
        --sftp-password=...
```

OR

```shell
$ kopia repository create sftp \
        --path=... \
        --host=... \
        --username=... \
        --keyfile=...
```

If the connection to SFTP server does not work, try adding `--external` which will launch an external `ssh` process that supports more connectivity options and may be needed for some hosts.

At a minimum, you will need to enter the path, host, username, and either password or path to key file. You may also need to include `--known-hosts`. There are also various other options (such as [actions](../advanced/actions/)) you can change or enable -- see the [help docs](../reference/command-line/common/repository-create-sftp/) for more information.

You will be asked to enter the repository password that you want. This password can be whatever you want, it does not need to be the same as your SFTP password. In fact, it should not be the same! Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password!

#### Connecting to Repository

After you have created the `repository`, you connect to it using the [`kopia repository connect sftp` command](../reference/command-line/common/repository-connect-sftp/). Read the [help docs](../reference/command-line/common/repository-connect-sftp/) for more information on the options available for this command.

## Rclone

[Rclone](https://rclone.org/) is an open-source program that allows you to connect to various cloud storage platforms. Many of these platforms are already supported natively by Kopia (see above), but some are not. If you want to use Kopia to backup to cloud storage that Rclone supports but Kopia does not yet, then you can use Kopia's Rclone `repository` feature to do just that. The best part is that once you setup the Rclone `repository`, Kopia manages Rclone for you (including running Rclone when needed), so you do not need to do anything else after setup except make sure you [enable Rclone's self-update feature](https://rclone.org/commands/rclone_selfupdate/) so that it stays up-to-date.

> WARNING: Rclone support is experimental. In theory, all Rclone-supported storage providers should work with Kopia. However, in practice, only Dropbox, OneDrive, and Google Drive have been tested to work with Kopia through Rclone.

Before you can create an Rclone `repository` in Kopia, you first need to download/install Rclone and setup what is called an Rclone `remote` for the cloud storage you want to use. Do the following:

1. Download Rclone from [the Rclone website](https://rclone.org/); it is a single executable like Kopia, so you do not need to install it but do remember the path on your machine where you save the Rclone executable file because you will need to know it when setting up your `repository` in Kopia
2. Configure Rclone to setup a `remote` to the storage provider you want to use Kopia with; see [Rclone help docs](https://rclone.org/docs/) to understand how to do that

### Kopia GUI

Select the `Rclone Remote` option in the `Repository` tab in `KopiaUI`. Then, follow on-screen instructions.  You will need to enter `Rclone Remote Path` and `Rclone Executable Path`. The `Remote Path` is `my-remote:/some/path`, where you should replace `my-remote` with the name of the Rclone `remote` you created earlier and replace `/some/path` with the directory on the cloud storage where you want Kopia to save your snapshots. The `Executable Path` is the location on your machine where you saved the Rclone executable that you downloaded earlier.

You will next need to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password! At this same password screen, you have the option to change the `Encryption` algorithm, `Hash` algorithm, `Splitter` algorithm, `Repository Format`, `Username`, and `Hostname`. Click the `Show Advanced Options` button to access these settings. If you do not understand what these settings are, do not change them because the default settings are the best settings.

> NOTE: Some settings, such as [actions](../advanced/actions/) and arguments to Rclone, can only be enabled when you create a new `repository` using command-line (see next section). However, once you create the `repository` via command-line, you can use the `repository` as normal in Kopia GUI: just connect to the `repository` as described above after you have created it in command-line.

Once you do all that, your repository should be created and you can start backing up your data! Remember, Kopia manages Rclone for you, so you do not need to do anything further with Rclone.

### Kopia CLI

#### Creating a Repository

You must use the [`kopia repository create rclone` command](../reference/command-line/common/repository-create-rclone/) to create a `repository` (assuming `my-remote` is the name of the Rclone `remote` you created earlier and `/some/path` is the directory on the cloud storage where you want Kopia to save your snapshots):

```shell
$ kopia repository create rclone --rclone-exe=/path/to/rclone/executable --remote-path=my-remote:/some/path
```

There are also various other options (such as [actions](../advanced/actions/) and arguments to Rclone) you can change or enable -- see the [help docs](../reference/command-line/common/repository-create-rclone/) for more information.

You will be asked to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password!

Remember, Kopia manages Rclone for you, so you do not need to do anything further with Rclone once you have created the `repository`.

#### Connecting to Repository

After you have created the `repository`, you connect to it using the [`kopia repository connect rclone` command](../reference/command-line/common/repository-connect-rclone/). Read the [help docs](../reference/command-line/common/repository-connect-rclone/) for more information on the options available for this command.

## Local or Network-attached Storage

Kopia allows you to save your snapshots on your local machine, network-attached, or any other readable directory that is attached to your local machine (such as USB device, SMB directory, SSHFS mount, etc.). All of these storages fall under the `filesystem` label.

Creating a filesystem `repository` is done differently depending on if you use Kopia GUI or Kopia CLI.

### Kopia GUI

Select the `Local Directory or NAS` option in the `Repository` tab in `KopiaUI`. Then, follow on-screen instructions.  You will need to enter `Directory Path`.

You will next need to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password! At this same password screen, you have the option to change the `Encryption` algorithm, `Hash` algorithm, `Splitter` algorithm, `Repository Format`, `Username`, and `Hostname`. Click the `Show Advanced Options` button to access these settings. If you do not understand what these settings are, do not change them because the default settings are the best settings.

> NOTE: Some settings, such as [actions](../advanced/actions/), can only be enabled when you create a new `repository` using command-line (see next section). However, once you create the `repository` via command-line, you can use the `repository` as normal in Kopia GUI: just connect to the `repository` as described above after you have created it in command-line.

Once you do all that, your repository should be created and you can start backing up your data!

### Kopia CLI

#### Creating a Repository

You must use the [`kopia repository create filesystem` command](../reference/command-line/common/repository-create-filesystem/) to create a `repository`:

```shell
$ kopia repository create filesystem --path=...
```

There are also various other options (such as [actions](../advanced/actions/)) you can change or enable -- see the [help docs](../reference/command-line/common/repository-create-filesystem/) for more information.

You will be asked to enter the repository password that you want. Remember, this [password is used to encrypt your data](../faqs/#how-do-i-enable-encryption), so make sure it is a secure password!

#### Connecting to Repository

After you have created the `repository`, you connect to it using the [`kopia repository connect filesystem` command](../reference/command-line/common/repository-connect-filesystem/). Read the [help docs](../reference/command-line/common/repository-connect-filesystem/) for more information on the options available for this command.
