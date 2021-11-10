---
title: "Installation"
linkTitle: "Installation"
weight: 20
---

### Installing Kopia

Kopia is an open source software (OSS) developed by a community on GitHub. It is available in two variants:

* `Command Line Interface (CLI)` which is a stand-alone binary called `kopia` and which can be used a terminal window or scripts. This is typically the preferred option for power users, system administrators, etc.

* `Graphical User Interface (GUI)`: It is a desktop application, called `KopiaUI`, that offers a friendly user interface.

The following installation options are available:

* [Official Releases](https://github.com/kopia/kopia/releases/latest)
* [Windows CLI (Scoop)](#windows-cli-installation-using-scoop)
* [Windows GUI](#windows-gui-installation)
* [Debian/Ubuntu Linux (APT Repository)](#linux-installation-using-apt-debian-ubuntu)
* [RedHat/CentOS/Fedora Linux (Linux YUM Repository)](#linux-installation-using-rpm-redhat-centos-fedora)
* [Arch Linux/Manjaro (AUR)](#linux-installation-using-aur-arch-manjaro)
* [macOS CLI Homebrew](#macos-cli-using-homebrew)
* [macOS GUI](#macos-gui-installer)
* [OpenBSD](#openbsd-installation-via-ports)
* [Docker Images](#docker-images)

If you like to test the latest unreleased version of Kopia:

* [Test Builds](https://github.com/kopia/kopia-test-builds/releases/latest) on GitHub
* [Windows CLI (Scoop)](#windows-cli-installation-using-scoop) offers `test-builds` bucket
* [macOS CLI Homebrew](#macos-cli-using-homebrew) offers `test-builds` TAP
* [Debian/Ubuntu Linux (APT Repository)](#linux-installation-using-apt-debian-ubuntu) offers `unstable` channel
* [RedHat/CentOS/Fedora Linux (Linux YUM Repository)](#linux-installation-using-rpm-redhat-centos-fedora) offers `unstable` channel
* [Source Code](https://github.com/kopia/kopia/) - see [compilation instructions](#compilation-from-source)

### Operating System Support

CLI and GUI packages are available for:

* Windows 7 or later, 64-bit (CLI binary, UI installer and Scoop package)
* macOS 10.11 or later, 64-bit (CLI binary, UI installer,  Homebrew package)
* Linux - `amd64`, `armhf` or `arm64` (CLI binary, RPM and DEB repositories)

### Windows CLI installation using Scoop

On Windows, Kopia CLI is available as a [Scoop](https://scoop.sh) package, which automates installation and upgrades.

Using Scoop, installing Kopia is as easy as:

```shell 
> scoop bucket add kopia https://github.com/kopia/scoop-bucket.git
> scoop install kopia
```

See the [Scoop Website](https://scoop.sh) for more information.

Alternatively, to install the latest unreleased version of Kopia use the following bucket instead:

```shell
> scoop bucket add kopia https://github.com/kopia/scoop-test-builds.git
```

### Windows GUI installation

Graphical installer of KopiaUI is available on the [Releases](https://github.com/kopia/kopia/releases/latest) page.
  
Simply download file named `KopiaUI-Setup-X.Y.Z.exe`, double click and follow on-screen prompts.

### macOS CLI using Homebrew

On macOS, you can use [Homebrew](https://brew.sh) to install and keep Kopia up-to-date.

To install:

```shell
$ brew install kopia/kopia/kopia
```

To upgrade Kopia:

```shell
$ brew upgrade kopia
```

Alternatively, to install the latest unreleased version of Kopia use the following TAP instead:

```shell
$ brew install kopia/test-builds/kopia
```

### macOS GUI installer

MacOS package with KopiaUI is available in DMG and ZIP formats on the [Releases](https://github.com/kopia/kopia/releases/latest) page.

### Linux installation using APT (Debian, Ubuntu)

Kopia offers APT repository compatible with Debian, Ubuntu and other similar distributions.

To begin, install the GPG signing key to verify authenticity of the releases.

```shell
curl -s https://kopia.io/signing-key | sudo apt-key add -
```

Register APT source:

```shell
echo "deb http://packages.kopia.io/apt/ stable main" | sudo tee /etc/apt/sources.list.d/kopia.list
sudo apt update
```

>By default, the **stable** channel provides official stable releases. If you prefer you can also select **testing** channel (which also provides release candidates and is generally stable) or **unstable** which includes all latest changes, but may not be stable.

Finally, install Kopia or KopiaUI:

```shell
sudo apt install kopia
sudo apt install kopia-ui
```

### Linux installation using RPM (RedHat, CentOS, Fedora)

Kopia offers RPM repository compatible with RedHat, CentOS, Fedora and other similar distributions.

To begin, install the GPG signing key to verify authenticity of the releases.

```shell
rpm --import https://kopia.io/signing-key
```

Install Yum repository:

```shell
cat <<EOF | sudo tee /etc/yum.repos.d/kopia.repo
[Kopia]
name=Kopia
baseurl=http://packages.kopia.io/rpm/stable/\$basearch/
gpgcheck=1
enabled=1
gpgkey=https://kopia.io/signing-key
EOF
```

>By default, the **stable** channel provides official stable releases. If you prefer you can also select **testing** channel (which also provides release candidates and is generally stable) or **unstable** which includes all latest changes, but may not be stable.

Finally, install Kopia or KopiaUI:

```shell
sudo yum install kopia
sudo yum install kopia-ui
```

### Linux installation using AUR (Arch, Manjaro)

Those using Arch-based distributions have the option of building Kopia from source or installing pre-complied binaries:

To build and install Kopia from source:

```shell
git clone https://aur.archlinux.org/kopia.git
cd kopia
makepkg -si
```

or if you use an AUR helper such as yay:

```shell
yay -S kopia
```

To install the binary version:

```shell
git clone https://aur.archlinux.org/kopia-bin.git
cd kopia-bin
makepkg -si
```

or if you use an AUR helper such as yay:

```shell
yay -S kopia-bin
```

### OpenBSD installation via ports

OpenBSD now has kopia in -current ports, which means it gets built as packages in snapshots for several platforms (amd64, arm64, mips64 and i386) and will appear as a package for OpenBSD 7.1 and later releases.

To install the kopia package, run:

```shell
# pkg_add kopia
```

To build Kopia from ports yourself, cd /usr/ports/sysutils/kopia and follow the [Ports](https://www.openbsd.org/faq/ports/ports.html) guide on building ports as usual.


### Verifying package integrity

When downloading from GitHub it's recommended to verify SHA256 checksum of the binary and comparing that to `checksums.txt`. For extra security you may want to verify that the checksums have been signed by official Kopia builder, by running GPG:

```shell
# Import official signing key
$ curl https://kopia.io/signing-key | gpg --import -

# Verify that file checksums are ok
$ sha256sum --check checksums.txt

# Verify signature file
$ gpg --verify checksums.txt.sig 
gpg: assuming signed data in 'checksums.txt'
gpg: Signature made Thu Apr 15 22:02:31 2021 PDT
gpg:                using RSA key 7FB99DFD47809F0D5339D7D92273699AFD56A556
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

### Docker Images

Kopia provides pre-built Docker container images for `amd64`, `arm64` and `arm` on [DockerHub](https://hub.docker.com/r/kopia/kopia).

The following tags are available:

* `latest` - tracks the latest stable release
* `testing` - tracks the latest stable or pre-release (such as a beta or release candidate)
* `unstable` - tracks the latest unstable nightly build
* `major.minor` - latest patch release for a given major and minor version (e.g. `0.8`)
* `major.minor.patch` - specific stable release

In order to run Kopia in a container, you must:

* provide repository password via `KOPIA_PASSWORD` environment variable
* mount `/app/config` directory in which Kopia will look for `repository.config` file
* (recommended) mount `/app/cache` directory in which Kopia will be keeping a cache of downloaded data
* (optional) mount `/app/logs` directory in which Kopia will be writing logs
* mount any data directory used for locally-attached repository

Invocation of `kopia/kopia` in a container will be similar to the following example: 

```shell
$ docker pull kopia/kopia:testing
$ docker run -e KOPIA_PASSWORD \
    -v /path/to/config/dir:/app/config \
    -v /path/to/cache/dir:/app/cache \
    kopia/kopia:testing snapshot list
```

(Adjust `testing` tag to the appropriate version)

>NOTE Kopia in container overrides default values of some environment variables, see https://github.com/kopia/kopia/blob/master/tools/docker/Dockerfile for more details.

Because Docker environment uses random hostnames it is recommended to explicitly set them using `--override-hostname` and `--override-username` parameters to Kopia when connecting
to a repository. The names will be persisted in a configuration file and used afterwards.

### Compilation From Source

If you have [Go 1.16](https://golang.org/) or newer, you may download and build Kopia yourself. No special setup is necessary, other than the Go compiler. You can simply run:

```shell
$ go get github.com/kopia/kopia
```

The resulting binary will be available in `$HOME/go/bin`. Note that this will produce basic binary that has all the features except support for HTML-based UI. To build full binary, download the source from GitHub and run:

```shell
$ make install
```

Additional information about building Kopia from source is available at https://github.com/kopia/kopia/blob/master/BUILD.md
