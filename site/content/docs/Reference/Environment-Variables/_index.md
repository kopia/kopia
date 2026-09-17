---
title: "Environment Variables"
linkTitle: "Environment Variables"
weight: 20
---

Many Kopia CLI flags can also be set with environment variables. This is useful when:

* You want to avoid putting secrets on the command line (where they appear in shell history and process listings)
* You run Kopia in containers or Kubernetes and inject configuration from Secrets or ConfigMaps
* You prefer a fully non-interactive, environment-driven workflow

When both a flag and its environment variable are set, the **flag takes precedence**.

> Most Kopia-specific variables use the `KOPIA_` prefix. Storage credentials often use the provider's standard names (for example `AWS_ACCESS_KEY_ID`).

## Common configuration

| Environment variable | Equivalent flag | Description |
| -------------------- | --------------- | ----------- |
| `KOPIA_PASSWORD` | `--password` / `-p` | Repository password |
| `KOPIA_CONFIG_PATH` | `--config-file` | Path to the repository configuration file |
| `KOPIA_CACHE_DIRECTORY` | `--cache-directory` | Local cache directory (connect / create) |
| `KOPIA_PERSIST_CREDENTIALS_ON_CONNECT` | `--persist-credentials` | Whether to persist credentials on connect (default `true`) |
| `KOPIA_CHECK_FOR_UPDATES` | `--check-for-updates` | Periodically check GitHub for updates (default `true`) |
| `KOPIA_USE_KEYRING` | `--use-keyring` | Store the repository password in the Gnome Keyring (Linux) |
| `KOPIA_BYTES_STRING_BASE_2` | — | If `true`, print sizes in binary (base-2) units instead of decimal |

### Example: password without CLI flags

```shell
export KOPIA_PASSWORD='your-repository-password'
kopia snapshot create /home/user/data
```

### Example: Kubernetes secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kopia-repo
type: Opaque
stringData:
  KOPIA_PASSWORD: "your-repository-password"
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: kopia-backup
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: kopia
              image: kopia/kopia
              args: ["snapshot", "create", "/data"]
              envFrom:
                - secretRef:
                    name: kopia-repo
              volumeMounts:
                - name: data
                  mountPath: /data
                  readOnly: true
```

## Repository server

Use these when running `kopia server` or connecting a client to a Kopia repository server. Prefer environment variables for passwords so they do not appear in `ps` output or shell history.

| Environment variable | Equivalent flag | Description |
| -------------------- | --------------- | ----------- |
| `KOPIA_SERVER_USERNAME` | `--server-username` | HTTP basic-auth username for the server (default `kopia`) |
| `KOPIA_SERVER_PASSWORD` | `--server-password` | HTTP basic-auth password for the server |
| `KOPIA_SERVER_CONTROL_USER` | `--server-control-username` | Control API username when starting the server |
| `KOPIA_SERVER_CONTROL_PASSWORD` | `--server-control-password` | Control API password when starting the server |
| `KOPIA_SERVER_ADDRESS` | `--address` | Server URL when connecting as a client (default `http://127.0.0.1:51515`) |
| `KOPIA_SERVER_CERT_FINGERPRINT` | `--server-cert-fingerprint` | Expected TLS certificate SHA-256 fingerprint |
| `KOPIA_AUTH_COOKIE_SIGNING_KEY` | `--auth-cookie-signing-key` | Fixed cookie signing key (advanced / hidden) |

### Example: start server with secrets from the environment

```shell
export KOPIA_PASSWORD='repo-password'
export KOPIA_SERVER_USERNAME='kopia'
export KOPIA_SERVER_PASSWORD='server-basic-auth-password'
export KOPIA_SERVER_CONTROL_USER='server-control'
export KOPIA_SERVER_CONTROL_PASSWORD='control-password'

kopia server start --insecure --address=0.0.0.0:51515
```

### Example: client connecting to the server

```shell
export KOPIA_SERVER_ADDRESS='https://kopia.example.com:51515'
export KOPIA_SERVER_USERNAME='kopia'
export KOPIA_SERVER_PASSWORD='server-basic-auth-password'
export KOPIA_SERVER_CERT_FINGERPRINT='AB:CD:...'

kopia repository connect server --url="$KOPIA_SERVER_ADDRESS"
```

## Snapshot and restore

| Environment variable | Equivalent flag | Description |
| -------------------- | --------------- | ----------- |
| `KOPIA_SNAPSHOT_FAIL_FAST` | `--fail-fast` | Fail quickly when creating a snapshot |
| `KOPIA_RESTORE_CONSISTENT_ATTRIBUTES` | `--consistent-attributes` | Fail restore if matching snapshots disagree on attributes |
| `KOPIA_NEW_PASSWORD` | `--new-password` | New password for `repository change-password` |
| `KOPIA_DIFF` | `--diff-command` | External diff command for `kopia diff` |

## Logging

Logging can also be tuned with flags; see [Logging](../../advanced/logging/) for full detail.

| Environment variable | Equivalent flag | Description |
| -------------------- | --------------- | ----------- |
| `KOPIA_LOG_DIR` | `--log-dir` | Directory for log files |
| `KOPIA_LOG_DIR_MAX_FILES` | `--log-dir-max-files` | Max CLI log files to retain (default `1000`) |
| `KOPIA_LOG_DIR_MAX_AGE` | `--log-dir-max-age` | Max age of CLI log files (default `720h`) |
| `KOPIA_LOG_DIR_MAX_SIZE_MB` | `--log-dir-max-total-size-mb` | Max total size of CLI logs in MB |
| `KOPIA_CONTENT_LOG_DIR_MAX_FILES` | `--content-log-dir-max-files` | Max content log files (default `5000`) |
| `KOPIA_CONTENT_LOG_DIR_MAX_AGE` | `--content-log-dir-max-age` | Max age of content log files (default `720h`) |
| `KOPIA_CONTENT_LOG_DIR_MAX_SIZE_MB` | `--content-log-dir-max-total-size-mb` | Max total size of content logs in MB |
| `KOPIA_LOG_FILE_MAX_SEGMENT_SIZE` | `--max-log-file-segment-size` | Max size of a single log segment |
| `KOPIA_FILE_LOG_LOCAL_TZ` | `--file-log-local-tz` | Use local timezone in file logs |
| `KOPIA_CONSOLE_TIMESTAMPS` | `--console-timestamps` | Print timestamps on stderr logs |
| `KOPIA_DISABLE_COLOR` | `--disable-color` | Disable color output |
| `KOPIA_FORCE_COLOR` | `--force-color` | Force color output |

## Cloud storage credentials

When creating or connecting to a repository, provider credentials can be supplied via environment variables instead of flags.

### Amazon S3 and S3-compatible

| Environment variable | Equivalent flag | Description |
| -------------------- | --------------- | ----------- |
| `AWS_ACCESS_KEY_ID` | `--access-key` | Access key ID |
| `AWS_SECRET_ACCESS_KEY` | `--secret-access-key` | Secret access key |
| `AWS_SESSION_TOKEN` | `--session-token` | Session token (optional) |
| `ROOT_CA_PEM_BASE64` | `--root-ca-pem-base64` | Custom CA certificate, base64-encoded |

### Azure Blob Storage

| Environment variable | Equivalent flag | Description |
| -------------------- | --------------- | ----------- |
| `AZURE_STORAGE_ACCOUNT` | `--storage-account` | Storage account name |
| `AZURE_STORAGE_KEY` | `--storage-key` | Storage account key |
| `AZURE_STORAGE_SAS_TOKEN` | `--sas-token` | SAS token |
| `AZURE_STORAGE_DOMAIN` | `--storage-domain` | Custom storage domain |
| `AZURE_TENANT_ID` | `--tenant-id` | Service principal tenant ID |
| `AZURE_CLIENT_ID` | `--client-id` | Service principal client ID |
| `AZURE_CLIENT_SECRET` | `--client-secret` | Service principal client secret |
| `AZURE_CLIENT_CERTIFICATE` | `--client-cert` | Client certificate |
| `AZURE_FEDERATED_TOKEN_FILE` | `--azure-federated-token-file` | Path to a federated token file |

### Backblaze B2

| Environment variable | Equivalent flag | Description |
| -------------------- | --------------- | ----------- |
| `B2_KEY_ID` | `--key-id` | Application key ID |
| `B2_KEY` | `--key` | Application key |

### WebDAV

| Environment variable | Equivalent flag | Description |
| -------------------- | --------------- | ----------- |
| `KOPIA_WEBDAV_USERNAME` | `--webdav-username` | WebDAV username |
| `KOPIA_WEBDAV_PASSWORD` | `--webdav-password` | WebDAV password |

## Observability

| Environment variable | Equivalent flag | Description |
| -------------------- | --------------- | ----------- |
| `KOPIA_METRICS_PUSH_ADDR` | `--metrics-push-addr` | Prometheus pushgateway address |
| `KOPIA_METRICS_PUSH_INTERVAL` | `--metrics-push-interval` | Push interval (default `5s`) |
| `KOPIA_METRICS_JOB` | `--metrics-push-job` | Job name (default `kopia`) |
| `KOPIA_METRICS_PUSH_GROUPING` | `--metrics-push-grouping` | Extra grouping labels |
| `KOPIA_METRICS_PUSH_USERNAME` | `--metrics-push-username` | Pushgateway username |
| `KOPIA_METRICS_PUSH_PASSWORD` | `--metrics-push-password` | Pushgateway password |
| `KOPIA_METRICS_FORMAT` | `--metrics-push-format` | Push format |
| `KOPIA_ENABLE_OTLP_TRACE` | `--otlp-trace` | Send OpenTelemetry traces via OTLP/gRPC |

## Feature gates and advanced options

These are uncommon or hidden; they are listed for completeness.

| Environment variable | Description |
| -------------------- | ----------- |
| `KOPIA_UPGRADE_LOCK_ENABLED` | Must be set to enable repository format upgrade lock features |
| `KOPIA_REPO_UPGRADE_OWNER_ID` | Owner ID for an in-progress format upgrade |
| `KOPIA_REPO_UPGRADE_NO_BLOCK` | Exit instead of blocking while an upgrade is in progress |
| `KOPIA_DANGEROUS_COMMANDS` | Enables dangerous commands that can corrupt the repository |
| `KOPIA_DISABLE_REPOSITORY_LOG` | Disable the repository log |
| `KOPIA_SEND_ERROR_NOTIFICATIONS` | Control error notification sending |
| `KOPIA_TRACK_RELEASABLE` | Track releasable resources (debug) |
| `KOPIA_DUMP_ALLOCATOR_STATS` | Dump allocator stats at process exit |
| `KOPIA_UI_TITLE_PREFIX` | UI title prefix when running the server UI |
| `KOPIA_INITIAL_UPDATE_CHECK_DELAY` | Delay before the first update check |
| `KOPIA_UPDATE_CHECK_INTERVAL` | Interval between update checks |
| `KOPIA_UPDATE_NOTIFY_INTERVAL` | Interval between update notifications |

## Discovering variables for a flag

Any flag that supports an environment variable accepts the same value through that variable. To see the flags for a command (and infer common `KOPIA_*` names from the flag help text in this page):

```shell
kopia help
kopia server start --help
kopia repository connect s3 --help
```

For flags defined in the CLI, the environment variable is typically the flag name in `UPPER_SNAKE_CASE` with a `KOPIA_` prefix (for example `--server-password` → `KOPIA_SERVER_PASSWORD`), unless the flag documents a provider-standard name such as `AWS_SECRET_ACCESS_KEY`.
