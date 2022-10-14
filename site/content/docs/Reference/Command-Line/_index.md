---
title: "Command Line"
linkTitle: "Command Line"
weight: 10
---

Kopia provides a command-line interface (CLI) for accessing all its functions. All commands are accessible through single binary called `kopia` (or `kopia.exe` on Windows).

Kopia functionality is organized into [Common Commands](common/) for typical use or [Advanced Commands](advanced/) for low-level data manipulation or recovery. Click on the above links for more details.

### Environment Variables

The following environment variables can be used to configure how Kopia runs:

| Variable Name         | Default | Description                                                                                              |
| --------------------- | ------- | -------------------------------------------------------------------------------------------------------- |
| `BYTES_STRING_BASE_2` | `false` | If set to `true`, Kopia will output storage values in binary (base-2). The default is decimal (base-10). |

### Connecting to Repository

Most commands require a [Repository](../../advanced/architecture/) to be connected first. The first time you use Kopia, repository must be created, later on it can be connected to from one or more machines.

Creating a repository is as simple as:

```shell
$ kopia repository create <provider> <flags>
```

Examples:

* [create repository in local filesystem](common/repository-create-filesystem/)
* [create repository in Google Cloud Storage bucket](common/repository-create-google/)
* [create repository in S3-compatible bucket](common/repository-create-s3/) (e.g. [Amazon S3](https://aws.amazon.com/s3/), [minio.io](https://minio.io/), [Wasabi](https://wasabi.com))

To connect to an existing repository use the same flags but instead of `create` use `connect`:

```shell
$ kopia repository connect <provider> <flags>
```

To disconnect:

```shell
$ kopia repository disconnect
```

### Quick Reconnection To Repository

To quickly reconnect to the repository on another machine, you can use `kopia repository status -t`, which will print quick-reconnect command that encodes all repository connection parameters in an opaque token. You can also embed the repository password, by using `kopia repository status -t -s`.

Such command can be stored long-term in a secure location, such as password manager for easy recovery.

```shell
$ kopia repository status -t -s
...

To reconnect to the repository use:

$ kopia repository connect from-config --token 03Fy598cYIqbMlNNDz9VLU0K6Pk9alC...BNeazLBdRzP2MHo0MS83zRb

NOTICE: The token printed above can be trivially decoded to reveal the repository password. Do not store it in an unsecured place.
```

> NOTE: Make sure to safeguard the repository token, as it gives full access to the repository to anybody in its possession.

### Configuration File

For each repository connection, Kopia maintains a configuration file and local cache:

By default, the configuration file is located in your home directory under:

* `%APPDATA%\kopia\repository.config` on Windows
* `$HOME/Library/Application Support/kopia/repository.config` on macOS
* `$HOME/.config/kopia/repository.config` on Linux

The location can be overridden using `--config-file`.

The configuration file stores the connection parameters, for example:

```json
{
  "storage": {
    "type": "s3",
    "config": {
      "bucket": "some-bucket",
      "endpoint": "s3.endpoint.com",
      "accessKeyID": "...",
      "secretAccessKey": "..."
    }
  },
  "caching": {
    "cacheDirectory": "<path-to>/kopia/7c04ae89ea31e77d-1",
    "maxCacheSize": 524288000,
    "maxListCacheDuration": 600
  }
}
```

The password to the repository is stored in operating-system specific credential storage (KeyChain on macOS, Credential Manager on Windows or KeyRing on Linux).
