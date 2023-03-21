---
title: "Logging"
linkTitle: "Logging"
weight: 60
---

## Logging

Kopia maintains diagnostic logging for troubleshooting purposes. This documents describes parameters that can be set to configure logging:

### Log File Location

The location of log directory varies by operating system:

* On Linux - `~/.cache/kopia`
* On macOS - `~/Library/Logs/kopia`
* On Windows - `%LocalAppData%\kopia`

Log file location can be overridden by setting flag `--log-dir` or `KOPIA_LOG_DIR` environment
variable.

The log directory contains two subdirectories:

* `cli-logs` - contains one log file per each invocation of `kopia` binary and contains general-purpose logging and debugging information and may contain sensitive information like username, hostname, filenames, etc. Please sanitize contents of such log files before filing bug reports.

* `content-logs` - contains one log file per each invocation of `kopia` binary and contains low-level formatting logs but will not contain any sensitive data such as file names, hostnames, etc.

### Log Retention

Log retention can be configured using flags and environment variables.

| Flag                              | Environment Variable         | Default | Description
| --------------------------------- | ---------------------------- | ------- | --------------
| `--log-dir-max-files`             | `KOPIA_LOG_DIR_MAX_FILES`    | 1000    | Maximum number of log files to retain |
| `--log-dir-max-age`               | `KOPIA_LOG_DIR_MAX_AGE`      | 720h    | Maximum age of log files to retain |
| `--content-log-dir-max-files`     | `KOPIA_CONTENT_LOG_DIR_MAX_FILES` | 5000 | Maximum number of content log files to retain | 
| `--content-log-dir-max-age`       | `KOPIA_CONTENT_LOG_DIR_MAX_AGE` | 720h | Maximum age of content log files to retain |

### Controlling Log Level

The amount of logs can be controlled using log levels:

* `debug` - most detailed logs including potentially verbose debugging information
* `info` - normal output
* `warning` - errors and warnings only
* `error` - errors only

You can control how much data is written to console and log files by using flags:

* `--log-level` - sets log level for console output (defaults to `info`)
* `--file-log-level` - sets log level for file output (defaults to `debug`)

### Color Output

By default, console output will be colored to indicate different log levels, this can be disabled (useful when redirecting output to a file) with `--disable-color`. To force color colorized output when redirecting to a file use `--force-color`.

