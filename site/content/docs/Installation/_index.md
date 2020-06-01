---
title: "Installation"
linkTitle: "Installation"
weight: 20
---

Kopia supports both command line (CLI) and graphical (GUI) user interfaces.

### Command Line Tool

The easiest way to get started with the Kopia CLI tool is to download pre-compiled `kopia` binary. Alternatively, you can compile it yourself from source. 

### Kopia UI

You can download installer packages of KopiaUI for Windows, macOS, and Linux from the [Releases](https://github.com/kopia/kopia/releases/latest) page on GitHub.

### Download

Kopia is released as a single, stand-alone binary, available for many operating systems:

* Windows 7 or later, 64-bit
* macOS 10.11 or later, 64-bit
* Linux - amd64, armv7 or arm64

To download latest binary for your platform, go to the [Releases](https://github.com/kopia/kopia/releases/latest) page on GitHub.

It's recommended to verify SHA256 checksum of the binary and comparing that to `checksums.txt`. For extra security you may want to verify that the checksums have been signed by official Kopia builder, by running GPG:

```shell
# Import official signing key
$ curl https://kopia.io/signing-key | gpg --import -

# Verify that file checksums are ok
$ sha256sum --check checksums.txt

# Verify signature file
$ gpg --verify checksums.txt.sig 
gpg: assuming signed data in 'checksums.txt'
gpg: Signature made Wed May 15 20:41:41 2019 PDT
gpg:                using RSA key A3B5843ED70529C23162E3687713E6D88ED70D9D
gpg: Good signature from "Kopia Builder <builder@kopia.io>" [ultimate]
```

You need to make the download binary executable (Linux/macOS only) and move it to a location that's in your system `PATH` for easy access:

On Linux/macOS run:
```shell
# make the file executable
$ chmod u+x path/to/kopia
# move to a location in system path
$ sudo mv path/to/kopia /usr/local/bin/kopia
```

Follow the [User Guide](/docs/user-guide/) for more information about using Kopia.

### macOS installation using Homebrew

On macOS you can use [Homebrew](https://brew.sh) to install and keep Kopia up-to-date.
Simply use `kopia/kopia` TAP:

```shell
$ brew tap kopia/kopia
$ brew install kopia
```

To upgrade Kopia:

```shell
$ brew upgrade kopia
```

### Linux installation using RPM (CentOS, Fedora, RedHat)

You can download a pre-built RPM package suitable for RedHat, Fedora and CentOS from the [Releases](https://github.com/kopia/kopia/releases/latest) page on GitHub and install it using:


```shell
$ rpm -Uhv kopia*.rpm
```

### Linux installation using DEB (Debian, Ubuntu)

You can download a pre-built DEB package suitable for Debian and Ubuntu Linux from the [Releases](https://github.com/kopia/kopia/releases/latest) page on GitHub and install it using:

```shell
$ deb -i kopia*.deb
```

### Compilation From Source

If you have [Go 1.13](https://golang.org/) or newer, you may download and build Kopia yourself. No special setup is necessary, other than the Go compiler. You can simply run:

```shell
$ go get github.com/kopia/kopia
```

The resulting binary will be available in `$HOME/go/bin`. Note that this will produce basic binary that has all the features except support for HTML-based UI. To build full binary, download the source from GitHub and run:

`make install` (on macOS/Linux) or `install.cmd` on Windows. Note that building on Windows also requires [NPM](https://nodejs.org/) to be installed.
