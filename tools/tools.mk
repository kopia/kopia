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

ifeq ($(GOOS),windows)
retry:=
endif

# tool versions
GOLANGCI_LINT_VERSION=1.62.0
CHECKLOCKS_VERSION=e8c1fff214d0ecf02cfe5aa9c62d11174130c339
NODE_VERSION=20.15.1
HUGO_VERSION=0.113.0
GOTESTSUM_VERSION=1.11.0
GORELEASER_VERSION=v0.176.0
RCLONE_VERSION=1.68.2
GITCHGLOG_VERSION=0.15.1

# nodejs / npm
node_base_dir=$(TOOLS_DIR)$(slash)node-$(NODE_VERSION)
ifeq ($(GOOS),windows)
node_dir=$(node_base_dir)
else
node_dir=$(node_base_dir)$(slash)bin
endif
npm=$(node_dir)$(slash)npm$(cmd_suffix)
npm_flags=--scripts-prepend-node-path=auto

npm_install_or_ci:=install
ifneq ($(CI),)
npm_install_or_ci:=ci
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
	go run github.com/kopia/kopia/tools/gettool -tool node:$(NODE_VERSION) --output-dir $(node_base_dir)
endif

# linter
linter_dir=$(TOOLS_DIR)$(slash)golangci-lint-$(GOLANGCI_LINT_VERSION)
linter=$(linter_dir)$(slash)golangci-lint$(exe_suffix)

linter_flags=
ifeq ($(GOOS),windows)
linter_flags=-D gofmt -D goimports
endif

$(linter):
	go run github.com/kopia/kopia/tools/gettool --tool linter:$(GOLANGCI_LINT_VERSION) --output-dir $(linter_dir)

# checklocks
checklocks_dir=$(TOOLS_DIR)$(slash)checklocks-$(CHECKLOCKS_VERSION)
checklocks=$(checklocks_dir)$(slash)bin$(slash)checklocks$(exe_suffix)

$(checklocks): export GOPATH=$(checklocks_dir)
$(checklocks):
	go install gvisor.dev/gvisor/tools/checklocks/cmd/checklocks@$(CHECKLOCKS_VERSION)
	go clean -modcache

# cli2md
cli2mdbin=$(TOOLS_DIR)$(slash)cli2md-current$(exe_suffix)

$(cli2mdbin):
	go build -o $(cli2mdbin) github.com/kopia/kopia/tools/cli2md

# hugo
hugo_dir=$(TOOLS_DIR)$(slash)hugo-$(HUGO_VERSION)
hugo=$(hugo_dir)/hugo$(exe_suffix)

$(hugo):
	go run github.com/kopia/kopia/tools/gettool --tool hugo:$(HUGO_VERSION) --output-dir $(hugo_dir)

# gitchglog
gitchglog_dir=$(TOOLS_DIR)$(slash)gitchglog-$(GITCHGLOG_VERSION)
gitchglog=$(gitchglog_dir)/git-chglog$(exe_suffix)

$(gitchglog):
	go run github.com/kopia/kopia/tools/gettool --tool gitchglog:$(GITCHGLOG_VERSION) --output-dir $(gitchglog_dir)

# rclone
rclone_dir=$(TOOLS_DIR)$(slash)rclone-$(RCLONE_VERSION)
rclone=$(rclone_dir)$(slash)rclone$(exe_suffix)
 
$(rclone):
	go run github.com/kopia/kopia/tools/gettool --tool rclone:$(RCLONE_VERSION) --output-dir $(rclone_dir)

# gotestsum
gotestsum_dir=$(TOOLS_DIR)$(slash)gotestsum-$(GOTESTSUM_VERSION)
gotestsum=$(gotestsum_dir)$(slash)gotestsum$(exe_suffix)

$(gotestsum):
	go run github.com/kopia/kopia/tools/gettool --tool gotestsum:$(GOTESTSUM_VERSION) --output-dir $(gotestsum_dir)

# kopia 0.8 for backwards compat testing
kopia08_version=0.8.4
kopia08_dir=$(TOOLS_DIR)$(slash)kopia-$(kopia08_version)
kopia08=$(kopia08_dir)$(slash)kopia$(exe_suffix)

$(kopia08):
	go run github.com/kopia/kopia/tools/gettool --tool kopia:$(kopia08_version) --output-dir $(kopia08_dir)

kopia017_version=0.17.0
kopia017_dir=$(TOOLS_DIR)$(slash)kopia-$(kopia017_version)
kopia017=$(kopia017_dir)$(slash)kopia$(exe_suffix)

$(kopia017):
	go run github.com/kopia/kopia/tools/gettool --tool kopia:$(kopia017_version) --output-dir $(kopia017_dir)

MINIO_MC_PATH=$(TOOLS_DIR)/bin/mc$(exe_suffix)

$(MINIO_MC_PATH):
	GOBIN=$(TOOLS_DIR)/bin go install github.com/minio/mc@latest

export MINIO_MC_PATH

wwhrd=$(TOOLS_DIR)/bin/wwhrd$(exe_suffix)

$(wwhrd):
	GOBIN=$(TOOLS_DIR)/bin go install github.com/frapposelli/wwhrd@latest

# goreleaser
goreleaser_dir=$(TOOLS_DIR)$(slash)goreleaser-$(GORELEASER_VERSION)
goreleaser=$(goreleaser_dir)$(slash)goreleaser$(exe_suffix)

$(goreleaser):
	go run github.com/kopia/kopia/tools/gettool --tool goreleaser:$(GORELEASER_VERSION) --output-dir $(goreleaser_dir)

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

KOPIA_BUILD_TAGS=
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

ALL_TOOL_VERSIONS=node:$(NODE_VERSION),linter:$(GOLANGCI_LINT_VERSION),hugo:$(HUGO_VERSION),rclone:$(RCLONE_VERSION),gotestsum:$(GOTESTSUM_VERSION),goreleaser:$(GORELEASER_VERSION),kopia:0.8.4,kopia:0.17.0,gitchglog:$(GITCHGLOG_VERSION)

verify-all-tool-checksums:
	go run github.com/kopia/kopia/tools/gettool --test-all \
	  --output-dir /tmp/all-tools \
	  --tool $(ALL_TOOL_VERSIONS)

regenerate-checksums:
	go run github.com/kopia/kopia/tools/gettool --regenerate-checksums $(CURDIR)/tools/gettool/checksums.txt \
	  --output-dir /tmp/all-tools \
	  --tool $(ALL_TOOL_VERSIONS)

all-tools: $(gotestsum) $(npm) $(linter) $(maybehugo)
