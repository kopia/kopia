---
title: "Download & Installation"
linkTitle: "Installation Guide"
weight: 20
---

## Two Variants of Kopia

Kopia is a standalone binary and can be used through a command-line interface (CLI) or a graphical user interface (GUI). 

* If you want to use Kopia via CLI, you will install the `kopia` binary; when you want to use Kopia, you will call the `kopia` binary (along with [Kopia commands](../reference/command-line/)) in a terminal/command prompt window or within a script. 

* If you want to use Kopia via GUI, you will install `KopiaUI`, the name of the Kopia GUI. The installer for KopiaUI comes with the `kopia` binary and a graphical user interface called `KopiaUI` - a wrapper for the `kopia` binary. `KopiaUI` runs the `kopia` binary and associated commands as necessary, so you do not need to use the command-line interface. 

> NOTE: `KopiaUI` is available both as a web-based application and a desktop application. The web-based application is available when you run Kopia in [server mode](../features/#optional-server-mode-with-api-support-to-centrally-manage-backups-of-multiple-machines). For users who will be using Kopia to backup their individual machines and not running Kopia in server mode, you will use the desktop application. If you do not understand Kopia server mode, do not worry; download `KopiaUI` from the [links below](#kopia-download-links), and you will get the desktop application by default.

Both the CLI and GUI versions of Kopia use the same `kopia` binary, so you are getting the same features regardless of which variant you decide to go with (since the `kopia` binary is the workhorse). However, some advanced features are available through CLI but have not yet been added to `KopiaUI`. Right now, `KopiaUI` allows you to access all the essential features of Kopia that are required to backup/restore data: create and connect to repositories (including encryption), set policies (including compression, scheduling automatic snapshots, and snapshot retention), create snapshots, restore snapshots, automatically run maintenance, and install Kopia updates. If you use `KopiaUI` and you want access to advanced features that are not yet available in `KopiaUI`, you can easily run the commands for those features via CLI by calling the `kopia` binary that comes with `KopiaUI`. In other words, using Kopia GUI does not restrict you from using Kopia CLI as well.

Kopia CLI is recommended only if you are comfortable with command-line interfaces (e.g., power users, system administrators, etc.). If you are uncomfortable with the command-line, use Kopia GUI. Although more limited than Kopia CLI, Kopia GUI is still very powerful and allows you to use Kopia to back up/restore your data easily.

## Kopia Download Links

The following installation options are available for the latest stable version of Kopia:

* [Official Releases](https://github.com/kopia/kopia/releases/latest)
* [Windows CLI (Scoop)](#windows-cli-installation-using-scoop)
* [Windows GUI (`KopiaUI`)](#windows-gui-installation)
* [macOS CLI Homebrew](#macos-cli-using-homebrew)
* [macOS GUI Homebrew](#macos-gui-using-homebrew)
* [macOS GUI (`KopiaUI`)](#macos-gui-installer)
* [Debian/Ubuntu Linux (APT Repository, both CLI and `KopiaUI`)](#linux-installation-using-apt-debian-ubuntu)
* [RedHat/CentOS/Fedora Linux (Linux YUM Repository, both CLI and `KopiaUI`)](#linux-installation-using-rpm-redhat-centos-fedora)
* [Arch Linux/Manjaro (AUR)](#linux-installation-using-aur-arch-manjaro)
* [OpenBSD](#openbsd-installation-via-ports)
* [FreeBSD](#freebsd-installation-via-ports)
* [Docker Images](#docker-images)

The following options are available if you like to test the beta and unreleased versions of Kopia:

* [Test Builds](https://github.com/kopia/kopia-test-builds/releases/latest) on GitHub
* [Windows CLI (Scoop)](#windows-cli-installation-using-scoop) offers `test-builds` bucket
* [macOS CLI Homebrew](#macos-cli-using-homebrew) offers `test-builds` TAP
* [Debian/Ubuntu Linux (APT Repository)](#linux-installation-using-apt-debian-ubuntu) offers `unstable` channel
* [RedHat/CentOS/Fedora Linux (Linux YUM Repository)](#linux-installation-using-rpm-redhat-centos-fedora) offers `unstable` channel
* [Source Code](https://github.com/kopia/kopia/) - see [compilation instructions](#compilation-from-source)

## Installing Kopia

CLI and GUI packages are available for:

* Windows 10 or later, 64-bit (CLI binary, GUI installer {`KopiaUI`}, and Scoop package)
* macOS 10.11 or later, 64-bit (CLI binary, GUI installer {`KopiaUI`}, and Homebrew package)
* Linux - `amd64`, `armhf` or `arm64` (CLI binary and `KopiaUI` available via RPM and DEB repositories)

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

The installer of `KopiaUI` is available on the [releases page](https://github.com/kopia/kopia/releases/latest). Simply download the file named `KopiaUI-Setup-X.Y.Z.exe` (where `X.Y.Z` is the version number), double click the file, and follow on-screen prompts.

### macOS CLI using Homebrew

On macOS, you can use [Homebrew](https://brew.sh) to install and keep Kopia up-to-date.

To install:

```shell
$ brew install kopia
```

To upgrade Kopia:

```shell
$ brew upgrade kopia
```

Alternatively, to install the latest unreleased version of Kopia use the following TAP instead:

```shell
$ brew install kopia/test-builds/kopia
```

### macOS GUI using Homebrew

On macOS, you can use [Homebrew](https://brew.sh) to install and keep Kopia up-to-date.

To install:

```shell
$ brew install kopiaui
```

To upgrade Kopia:

```shell
$ brew upgrade kopiaui
```

### macOS GUI installer

MacOS package with `KopiaUI` is available in DMG and ZIP formats on the [releases page](https://github.com/kopia/kopia/releases/latest).

### Linux installation using APT (Debian, Ubuntu)

Kopia offers APT repository compatible with Debian, Ubuntu and other similar distributions.

To begin, install the GPG signing key to verify authenticity of the releases.

```shell
curl -s https://kopia.io/signing-key | sudo gpg --dearmor -o /etc/apt/keyrings/kopia-keyring.gpg
```

Register APT source:

```shell
echo "deb [signed-by=/etc/apt/keyrings/kopia-keyring.gpg] http://packages.kopia.io/apt/ stable main" | sudo tee /etc/apt/sources.list.d/kopia.list
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

> By default, the **stable** channel provides official stable releases. If you prefer you can also select **testing** channel (which also provides release candidates and is generally stable) or **unstable** which includes all latest changes, but may not be stable.

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

OpenBSD has kopia in ports, which means it gets built as packages in snapshots for several platforms (amd64, arm64, mips64 and i386).

To install the kopia package, run:

```shell
# pkg_add kopia
```

To build Kopia from ports yourself, cd /usr/ports/sysutils/kopia and follow the [Ports](https://www.openbsd.org/faq/ports/ports.html) guide on building ports as usual.

### FreeBSD installation via ports

FreeBSD now has kopia in ports, which means it gets built as packages in snapshots for several platforms (amd64, arm64 and i386) and will appear as a package for supported versions.

To install the port:

```shell
cd /usr/ports/sysutils/kopia/ && make install clean
```

To add the package, run one of these commands:

```shell
pkg install sysutils/kopia
pkg install kopia
```

For more information on ports, see the [FreeBSD Handbook](https://docs.freebsd.org/en/books/handbook/ports/index.html#ports).

### Docker Images

Kopia provides pre-built Docker container images for `amd64`, `arm64` and `arm` on [DockerHub](https://hub.docker.com/r/kopia/kopia).

The following tags are available:

* `latest` - tracks the latest stable release
* `testing` - tracks the latest stable or pre-release (such as a beta or release candidate)
* `unstable` - tracks the latest unstable nightly build
* `major.minor` - latest patch release for a given major and minor version (e.g. `0.8`)
* `major.minor.patch` - specific stable release

In order to run Kopia in a docker container, you must:

* provide repository password via `KOPIA_PASSWORD` environment variable
* mount `/app/config` directory in which Kopia will look for `repository.config` file
* (recommended) mount `/app/cache` directory in which Kopia will be keeping a cache of downloaded data
* (optional) mount `/app/logs` directory in which Kopia will be writing logs
* (optional), **only** when using `rclone` provider mount `/app/rclone` directory in which RClone will look for `rclone.conf` file
* mount any directory used for locally-attached `repository`
* mount `/tmp` directory to browse mounted snapshots
    * the directory must have `:shared` property, so mounts can be browsable by host system
* for nginx reverse proxy, use: `grpc_pass grpcs://container_ip:container_port` instead of `proxy_pass`

Invocation of `kopia/kopia` in a container will be similar to the following minimal example: 

```shell
$ docker pull kopia/kopia:latest
$ docker run -e KOPIA_PASSWORD \
    -v /path/to/config/dir:/app/config \
    -v /path/to/cache/dir:/app/cache \
    -v /path/to/logs/dir:/app/logs \
    -v /path/to/repository/dir:/repository \
    -v /path/to/tmp/dir:/tmp:shared \
```

In addition to creating the docker container with *docker run*, the following docker-compose provides an example for setting up a minimal container in [server mode](../features/#optional-server-mode-with-api-support-to-centrally-manage-backups-of-multiple-machines) including the web interface. You can access the interface via http://localhost:51515 or at the server's IP address after starting the container.  

>NOTE Kopia provides a vast of parameters to configure the container. Please check our [docker-compose](https://github.com/kopia/kopia/blob/master/tools/docker/docker-compose.yml) for more details.

```shell
version: '3.7'
services:
    kopia:
        image: kopia/kopia:latest
        hostname: Hostname
        container_name: Kopia
        restart: unless-stopped
        ports:
            - 51515:51515
        # Setup the server that provides the web gui
        command:
            - server
            - start
            - --disable-csrf-token-checks
            - --insecure
            - --address=0.0.0.0:51515
            - --server-username=USERNAME
            - --server-password=SECRET_PASSWORD
        environment:
            # Set repository password
            KOPIA_PASSWORD: "SECRET"
            USER: "User"
        volumes:
            # Mount local folders needed by kopia
            - /path/to/config/dir:/app/config
            - /path/to/cache/dir:/app/cache
            - /path/to/logs/dir:/app/logs
            # Mount local folders to snapshot
            - /path/to/data/dir:/data:ro
            # Mount repository location
            - /path/to/repository/dir:/repository
            # Mount path for browsing mounted snaphots
            - /path/to/tmp/dir:/tmp:shared
```

Because the Docker environment uses random hostnames for its containers, it is recommended to explicitly set them using `hostname`. The name will be persisted in a configuration file and used afterwards.

>NOTE Kopia within a container overrides default values of some environment variables, see our [dockerfile](https://github.com/kopia/kopia/blob/master/tools/docker/Dockerfile) for more details.

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

### Compilation From Source

If you have [Go 1.16](https://golang.org/) or newer, you may download and build Kopia yourself. No special setup is necessary, other than the Go compiler and [git](https://git-scm.com/). You can simply run:

```shell
$ go install github.com/kopia/kopia@latest
```

The resulting binary will be available in `$HOME/go/bin`. Note that this will produce basic binary that has all the features except support for HTML-based UI. To build full binary, download the source from GitHub and run:

```shell
$ make install
```

Additional information about building Kopia from source is available at https://github.com/kopia/kopia/blob/master/BUILD.md
