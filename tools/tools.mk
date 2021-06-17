# to build on Windows you need the following basic Unix tools in your PATH installed using Chocolatey
#
# make
# unzip
# curl
#
# you will need to have git and golang too in the PATH.

# windows,linux,darwin
GOOS:=$(shell go env GOOS)
# amd64,arm64,arm
GOARCH:=$(shell go env GOARCH)

# uname will be Windows, Darwin, Linux
ifeq ($(GOOS),windows)
	exe_suffix := .exe
	cmd_suffix := .cmd
	uname := Windows
	slash=\\
	path_separator=;
	date_ymd := $(shell powershell -noprofile -executionpolicy bypass -Command "(Get-Date).ToString('yyyyMMdd')")
	date_full := $(shell powershell -noprofile -executionpolicy bypass -Command "(Get-Date).datetime")
	raw_arch:=$(GOARCH)
	hostname:=$(COMPUTERNAME)

ifeq ($(UNIX_SHELL_ON_WINDOWS),true)
	mkdir=mkdir -p
	move=mv
	slash=/
else
	mkdir=md
	move=move
endif

else
	uname := $(shell uname -s)
	exe_suffix :=
	cmd_suffix :=
	slash=/
	path_separator=:
	mkdir=mkdir -p
	date_ymd := $(shell date +%Y%m%d)
	date_full := $(shell date)
	raw_arch:=$(shell uname -m)
	hostname:=$(shell hostname)
endif

ifneq ($(GITHUB_ACTIONS),)

# running on GitHub actions.

ifeq ($(GITHUB_HEAD_REF),)
export IS_PULL_REQUEST=false
else
export IS_PULL_REQUEST=true
endif

# try parsing tag name out of GITHUB_REF
gh_tag_tmp=$(GITHUB_REF:refs/tags/%=%)

ifneq ($(gh_tag_tmp),$(GITHUB_REF))
export CI_TAG=$(gh_tag_tmp)
endif

endif

# detect REPO_OWNER, e.g. 'kopia' for official builds
export REPO_OWNER=unknown-repo-owner
ifneq ($(GITHUB_REPOSITORY),)
export REPO_OWNER=$(GITHUB_REPOSITORY:%/kopia=%)
endif

# e.g. 2021-02-19 06:56:21 -0800
git_commit_date:=$(shell git show -s --format=%ci HEAD)

# compute build date and time from the current commit as yyyyMMdd
commit_date_ymd:=$(subst -,,$(word 1, $(git_commit_date)))

# compute time of day as a decimal number, without leading zeroes
# midnight will be 0
# 00:01:00 becomes 100
# 00:10:00 becomes 1000
# 07:00:00 becomes 70000
# end of day is 235959
# time of day as hhmmss from 000000 to 235969
commit_time_raw:=$(subst :,,$(word 2, $(git_commit_date)))
commit_time_stripped1=$(commit_time_raw:0%=%)
commit_time_stripped2=$(commit_time_stripped1:0%=%)
commit_time_stripped3=$(commit_time_stripped2:0%=%)
commit_time_stripped4=$(commit_time_stripped3:0%=%)

# final time of day number
commit_time_of_day=$(commit_time_stripped4:0%=%)

SELF_DIR := $(subst /,$(slash),$(realpath $(dir $(lastword $(MAKEFILE_LIST)))))
TOOLS_DIR:=$(SELF_DIR)$(slash).tools

retry:=$(SELF_DIR)/retry.sh

# tool versions
GOLANGCI_LINT_VERSION=1.41.0
NODE_VERSION=14.15.4
HUGO_VERSION=0.82.0
GOTESTSUM_VERSION=0.5.3
GORELEASER_VERSION=v0.158.0
RCLONE_VERSION=1.53.4

# nodejs / npm
node_base_dir=$(TOOLS_DIR)$(slash)node-$(NODE_VERSION)
node_dir=$(node_base_dir)$(slash)node$(slash)bin
npm=$(node_dir)$(slash)npm$(cmd_suffix)
npm_flags=--scripts-prepend-node-path=auto
node_arch_name=x64
ifeq ($(raw_arch),aarch64)
	node_arch_name=arm64
endif
ifeq ($(raw_arch),armv7l)
	node_arch_name=armv7l
endif

# put NPM in the path
PATH:=$(node_dir)$(path_separator)$(PATH)
ifeq ($(GOOS),$(filter $(GOOS),openbsd freebsd))
npm=/usr/local/bin/npm
endif

$(npm):
ifeq ($(GOOS),openbsd)
	@echo Use pkg_add to install node
	@exit 1
else ifeq ($(GOOS),freebsd)
	@echo Use pkg to install npm
	@exit 1
else
	@echo Downloading Node v$(NODE_VERSION) with NPM path $(npm)
	$(mkdir) $(node_base_dir)$(slash)node

ifeq ($(GOOS),windows)
	curl -Ls -o $(node_base_dir).zip https://nodejs.org/dist/v$(NODE_VERSION)/node-v$(NODE_VERSION)-win-x64.zip
	unzip -q $(node_base_dir).zip -d $(node_base_dir)
	$(move) $(node_base_dir)\\node-v$(NODE_VERSION)-win-x64 $(node_base_dir)\\node\\bin
else

ifeq ($(GOOS),linux)
	curl -LsS https://nodejs.org/dist/v$(NODE_VERSION)/node-v$(NODE_VERSION)-linux-$(node_arch_name).tar.gz | tar zx -C $(node_base_dir)
else
	curl -LsS https://nodejs.org/dist/v$(NODE_VERSION)/node-v$(NODE_VERSION)-darwin-x64.tar.gz | tar zx -C $(node_base_dir)
endif
	mv $(node_base_dir)/node-v$(NODE_VERSION)*/* $(node_base_dir)/node
endif
endif

# linter
linter_dir=$(TOOLS_DIR)$(slash)golangci-lint-$(GOLANGCI_LINT_VERSION)
linter=$(linter_dir)$(slash)golangci-lint$(exe_suffix)
linter_flags=
linter_arch_name=amd64
ifeq ($(raw_arch),aarch64)
	linter_arch_name=arm64
endif

ifeq ($(raw_arch),armv7l)
	linter_arch_name=armv7
endif


ifeq ($(GOOS),windows)
linter_flags=-D gofmt -D goimports
endif

$(linter):
	@echo Downloading GolangCI-lint v$(GOLANGCI_LINT_VERSION) to $(linter)
ifeq ($(GOOS),windows)
	-$(mkdir) $(linter_dir)
	curl -LsS -o $(linter_dir).zip https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_LINT_VERSION)/golangci-lint-$(GOLANGCI_LINT_VERSION)-windows-amd64.zip
	unzip -q $(linter_dir).zip -d $(linter_dir)
	$(move) $(linter_dir)\golangci-lint-$(GOLANGCI_LINT_VERSION)-windows-amd64\golangci-lint.exe $(linter)
else
	mkdir -p $(linter_dir)
ifeq ($(GOOS),linux)
	curl -LsS https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_LINT_VERSION)/golangci-lint-$(GOLANGCI_LINT_VERSION)-linux-$(linter_arch_name).tar.gz | tar zxv --strip=1 -C $(linter_dir)
else
	curl -LsS https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_LINT_VERSION)/golangci-lint-$(GOLANGCI_LINT_VERSION)-darwin-amd64.tar.gz | tar zxv --strip=1 -C $(linter_dir)
endif

endif

# hugo
hugo_dir=$(TOOLS_DIR)$(slash)hugo-$(HUGO_VERSION)
hugo=$(hugo_dir)$(slash)hugo$(exe_suffix)

$(hugo):
	@echo Downloading Hugo v$(HUGO_VERSION) to $(hugo)
	-$(mkdir) $(TOOLS_DIR)$(slash)hugo-$(HUGO_VERSION)

ifeq ($(GOOS),windows)
	curl -LsS -o $(hugo_dir).zip https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_Windows-64bit.zip
	unzip -q $(hugo_dir).zip -d $(hugo_dir)
else

ifeq ($(GOOS),linux)
	curl -LsS https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_Linux-64bit.tar.gz | tar zxv -C $(hugo_dir)
else
	curl -LsS https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_macOS-64bit.tar.gz | tar zxv -C $(hugo_dir)
endif

endif

# rclone
rclone_os_name:=$(GOOS)
ifeq ($(GOOS),darwin)
rclone_os_name=osx
endif

rclone_dir=$(TOOLS_DIR)$(slash)rclone-$(RCLONE_VERSION)
rclone=$(rclone_dir)$(slash)rclone$(exe_suffix)

$(rclone):
	@echo Downloading RCLONE_VERSION v$(RCLONE_VERSION) to $(rclone)
	-$(mkdir) $(TOOLS_DIR)$(slash)rclone-$(RCLONE_VERSION)

	curl -LsS -o $(rclone_dir).zip https://github.com/rclone/rclone/releases/download/v$(RCLONE_VERSION)/rclone-v$(RCLONE_VERSION)-$(rclone_os_name)-$(GOARCH).zip
	unzip -j -q $(rclone_dir).zip -d $(rclone_dir)

# gotestsum
gotestsum=$(TOOLS_DIR)/bin/gotestsum$(exe_suffix)

$(gotestsum): export GO111MODULE=off
$(gotestsum): export GOPATH=$(TOOLS_DIR)
$(gotestsum):
	-$(mkdir) $(TOOLS_DIR)
	go get gotest.tools/gotestsum

# goreleaser
goreleaser_dir=$(TOOLS_DIR)$(slash)goreleaser-$(GORELEASER_VERSION)
goreleaser=$(goreleaser_dir)$(slash)goreleaser$(exe_suffix)

goreleaser_arch_name=x86_64
ifeq ($(raw_arch),aarch64)
	goreleaser_arch_name=arm64
endif

ifeq ($(raw_arch),armv7l)
	goreleaser_arch_name=armv6
endif

$(goreleaser):
	@echo Downloading GoReleaser $(GORELEASER_VERSION) to $(goreleaser)
	-$(mkdir) $(goreleaser_dir)
ifeq ($(GOOS),windows)
	curl -LsS -o $(goreleaser_dir).zip https://github.com/goreleaser/goreleaser/releases/download/$(GORELEASER_VERSION)/goreleaser_Windows_x86_64.zip
	unzip -q $(goreleaser_dir).zip -d $(goreleaser_dir)
else
	curl -LsS https://github.com/goreleaser/goreleaser/releases/download/$(GORELEASER_VERSION)/goreleaser_$$(uname -s)_$(goreleaser_arch_name).tar.gz | tar zx -C $(TOOLS_DIR)/goreleaser-$(GORELEASER_VERSION)
endif

ifeq ($(IS_PULL_REQUEST),false)

ifneq ($(CI_TAG),)
# CI, tagged release
KOPIA_VERSION:=$(CI_TAG)
else
# CI, non-tagged release
KOPIA_VERSION:=v$(commit_date_ymd).0.$(commit_time_of_day)
endif

else

# non-CI, or CI in PR mode
KOPIA_VERSION:=v$(date_ymd).0.0-$(shell git rev-parse --short HEAD)

endif

export KOPIA_VERSION_NO_PREFIX=$(KOPIA_VERSION:v%=%)

# embedded in the HTML pages
export REACT_APP_SHORT_VERSION_INFO:=$(KOPIA_VERSION)
export REACT_APP_FULL_VERSION_INFO:=$(KOPIA_VERSION) built on $(date_full) $(hostname)

KOPIA_BUILD_FLAGS=-ldflags "-s -w -X github.com/kopia/kopia/repo.BuildVersion=$(KOPIA_VERSION_NO_PREFIX) -X github.com/kopia/kopia/repo.BuildInfo=$(shell git rev-parse HEAD) -X github.com/kopia/kopia/repo.BuildGitHubRepo=$(GITHUB_REPOSITORY)"

clean-tools:
	rm -rf $(TOOLS_DIR)

windows_signing_dir=$(TOOLS_DIR)$(slash)win_signing

# name of the temporary keychain to import signing keys into (will be deleted and re-created by 'signing-tools' target)
MACOS_KEYCHAIN=kopia-build.keychain
export CSC_KEYCHAIN:=$(MACOS_KEYCHAIN)
export CSC_NAME:=$(MACOS_SIGNING_IDENTITY)

windows-signing-tools:
ifeq ($(GOOS)/$(CI),windows/true)
ifneq ($(WINDOWS_SIGNING_TOOLS_URL),)
	echo Installing Windows signing tools to $(windows_signing_dir)...
	-$(mkdir) $(windows_signing_dir)
	curl -LsS -o $(windows_signing_dir).zip $(WINDOWS_SIGNING_TOOLS_URL)
	unzip -a -q $(windows_signing_dir).zip -d $(windows_signing_dir)
	pwsh -noprofile -executionpolicy bypass $(windows_signing_dir)\\setup.ps1
else
	@echo Not installing Windows signing tools because WINDOWS_SIGNING_TOOLS_URL is not set
endif
endif

# create and unlock a keychain with random strong password and import macOS signing certificate from .p12.
ifeq ($(GOOS)/$(CI),darwin/true)
macos-certificates: KEYCHAIN_PASSWORD:=$(shell uuidgen)
endif
macos-certificates:
ifneq ($(CSC_LINK),)
	@rm -fv $(HOME)/Library/Keychains/$(MACOS_KEYCHAIN)-db
	@echo "$(CSC_LINK)" | base64 -d > /tmp/certs.p12
	@security create-keychain -p $(KEYCHAIN_PASSWORD) $(MACOS_KEYCHAIN)
	@security unlock-keychain -p $(KEYCHAIN_PASSWORD) $(MACOS_KEYCHAIN)
	@security list-keychain -s $(MACOS_KEYCHAIN) login.keychain
	@security import /tmp/certs.p12 -k $(MACOS_KEYCHAIN) -P $(CSC_KEY_PASSWORD) -T /usr/bin/codesign;
	@security set-keychain-settings -u $(MACOS_KEYCHAIN)
	@rm -f /tmp/certs.p12
	@security set-key-partition-list -S apple: -s -k $(KEYCHAIN_PASSWORD) $(MACOS_KEYCHAIN) > /dev/null
else
	@echo Not installing macOS certificates because CSC_LINK is not set.
endif

# disable some tools on non-default architectures
ifeq ($(GOARCH),amd64)
maybehugo=$(hugo)
else
maybehugo=
endif

all-tools: $(gotestsum) $(npm) $(linter) $(maybehugo)

