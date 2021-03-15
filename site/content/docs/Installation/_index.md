---
title: "Installation"
linkTitle: "Installation"
weight: 20
---

### Installing Kopia 

Kopia is an open source software (OSS) developed by a community on GitHub.

The recommended way of installing Kopia is to use a package manager for your operating system (YUM or APT for Linux, Homebrew for macOS, Scoop for Windows). They offer quick and convenient way of installing and keeping Kopia up-to-date. See below for more information.

You can also download the [Source Code](https://github.com/kopia/kopia/) or [Binary Releases](https://github.com/kopia/kopia/releases/latest) directly from GitHub. 

Kopia is available in two variants:

* `Command Line Interface (CLI)` which is a stand-alone binary called `kopia` and which can be used a terminal window or scripts. This is typically the preferred option for power users, system administrators, etc.

* `Graphical User Interface (GUI)`: It is a desktop application, called `KopiaUI`, that offers a friendly user interface.

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

### Windows GUI installation

Graphical installer of KopiaUI is available on the [Releases](https://github.com/kopia/kopia/releases/latest) page.
  
Simply download file named `KopiaUI-Setup-X.Y.Z.exe`, double click and follow on-screen prompts.

### macOS CLI using Homebrew

On macOS you can use [Homebrew](https://brew.sh) to install and keep Kopia up-to-date.

To install:

```shell
$ brew tap kopia/kopia
$ brew install kopia
```

To upgrade Kopia:

```shell
$ brew upgrade kopia
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

>By default the **stable** channel provides official stable releases. If you prefer you can also select **testing** channel (which also provides release candidates and is generally stable) or **unstable** which includes all latest changes, but may not be stable.

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

>By default the **stable** channel provides official stable releases. If you prefer you can also select **testing** channel (which also provides release candidates and is generally stable) or **unstable** which includes all latest changes, but may not be stable.

Finally install Kopia or KopiaUI:

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

### Compilation From Source

If you have [Go 1.15](https://golang.org/) or newer, you may download and build Kopia yourself. No special setup is necessary, other than the Go compiler. You can simply run:

```shell
$ go get github.com/kopia/kopia
```

The resulting binary will be available in `$HOME/go/bin`. Note that this will produce basic binary that has all the features except support for HTML-based UI. To build full binary, download the source from GitHub and run:

Additional information about building Kopia from source is available at https://github.com/kopia/kopia/blob/master/BUILD.md
