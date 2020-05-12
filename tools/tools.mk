# to build on Windows you need the following basic Unix tools in your PATH installed using Chocolatey
#
# make
# unzip
# curl
#
# you will need to have git and golang too in the PATH.

ifeq ($(TRAVIS_OS_NAME),windows)
UNIX_SHELL_ON_WINDOWS=true
endif

ifneq ($(APPVEYOR),)

UNIX_SHELL_ON_WINDOWS=false

# running Windows build on AppVeyor
# emulate Travis CI environment variables, so we can use TRAVIS logic everywhere

ifeq ($(APPVEYOR_PULL_REQUEST_NUMBER),)
export TRAVIS_PULL_REQUEST=false
else
export TRAVIS_PULL_REQUEST=$(APPVEYOR_PULL_REQUEST_NUMBER)
endif

ifneq ($(APPVEYOR_REPO_TAG_NAME),)
export TRAVIS_TAG=$(APPVEYOR_REPO_TAG_NAME)
endif

TRAVIS_OS_NAME=windows

endif

# uname will be Windows, Darwin, Linux
ifeq ($(OS),Windows_NT)
	exe_suffix := .exe
	cmd_suffix := .cmd
	uname := Windows
	slash=\\
	path_separator=;
	date_ymd := $(shell powershell -noprofile -executionpolicy bypass -Command "(Get-Date).ToString('yyyyMMdd')")
	date_full := $(shell powershell -noprofile -executionpolicy bypass -Command "(Get-Date).datetime")
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
endif

# compute build date and time from the current commit
commit_date_ymd:=$(subst -,,$(word 1, $(shell git show -s --format=%ci HEAD)))

# compute time of day as a decimal number, without leading zeroes
# midnight will be 0
# 00:01:00 becomes 100
# 00:10:00 becomes 1000
# 07:00:00 becomes 70000
# end of day is 235959
# time of day as hhmmss from 000000 to 235969
commit_time_raw:=$(subst :,,$(word 2, $(shell git show -s --format=%ci HEAD)))
commit_time_stripped1=$(commit_time_raw:0%=%)
commit_time_stripped2=$(commit_time_stripped1:0%=%)
commit_time_stripped3=$(commit_time_stripped2:0%=%)
commit_time_stripped4=$(commit_time_stripped3:0%=%)

# final time of day number
commit_time_of_day=$(commit_time_stripped4:0%=%)


SELF_DIR := $(subst /,$(slash),$(realpath $(dir $(lastword $(MAKEFILE_LIST)))))
TOOLS_DIR:=$(SELF_DIR)$(slash).tools

# tool versions
GOLANGCI_LINT_VERSION=1.23.7
NODE_VERSION=12.16.1
HUGO_VERSION=0.67.1
GORELEASER_VERSION=v0.128.0

# goveralls
GOVERALLS_TOOL=$(TOOLS_DIR)/bin/goveralls

$(GOVERALLS_TOOL):
	$(mkdir) $(TOOLS_DIR)
	GO111MODULE=off GOPATH=$(TOOLS_DIR) go get github.com/mattn/goveralls

# nodejs / npm
node_base_dir=$(TOOLS_DIR)$(slash)node-$(NODE_VERSION)
node_dir=$(node_base_dir)$(slash)node$(slash)bin
npm=$(node_dir)$(slash)npm$(cmd_suffix)
npm_flags=--scripts-prepend-node-path=auto

ifneq ($(uname),Windows)
PATH:=$(node_dir)$(path_separator)$(PATH)
endif

ifeq ($(TRAVIS_OS_NAME),windows)
PATH:=$(node_dir)$(path_separator)$(PATH)
endif

$(npm):
	@echo Downloading Node v$(NODE_VERSION) with NPM path $(npm)
	$(mkdir) $(node_base_dir)$(slash)node

ifeq ($(uname),Windows)
	curl -Ls -o $(node_base_dir).zip https://nodejs.org/dist/v$(NODE_VERSION)/node-v$(NODE_VERSION)-win-x64.zip
	unzip -q $(node_base_dir).zip -d $(node_base_dir)
	$(move) $(node_base_dir)\\node-v$(NODE_VERSION)-win-x64 $(node_base_dir)\\node\\bin
else

ifeq ($(uname),Linux)
	curl -LsS https://nodejs.org/dist/v$(NODE_VERSION)/node-v$(NODE_VERSION)-linux-x64.tar.gz | tar zx -C $(node_base_dir)
else
	curl -LsS https://nodejs.org/dist/v$(NODE_VERSION)/node-v$(NODE_VERSION)-darwin-x64.tar.gz | tar zx -C $(node_base_dir)
endif
	mv $(node_base_dir)/node-v$(NODE_VERSION)*/* $(node_base_dir)/node
endif

go_bindata=$(TOOLS_DIR)$(slash)bin$(slash)go-bindata$(exe_suffix)

$(go_bindata): export GO111MODULE=off
$(go_bindata): export GOPATH=$(TOOLS_DIR)
$(go_bindata):
	go get github.com/go-bindata/go-bindata/go-bindata

# linter
linter_dir=$(TOOLS_DIR)$(slash)golangci-lint-$(GOLANGCI_LINT_VERSION)
linter=$(linter_dir)$(slash)golangci-lint$(exe_suffix)
linter_flags=

ifeq ($(uname),Windows)
linter_flags=-D gofmt -D goimports
endif

$(linter):
	@echo Downloading GolangCI-lint v$(GOLANGCI_LINT_VERSION) to $(linter)
ifeq ($(uname),Windows)
	-$(mkdir) $(linter_dir)
	curl -LsS -o $(linter_dir).zip https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_LINT_VERSION)/golangci-lint-$(GOLANGCI_LINT_VERSION)-windows-amd64.zip
	unzip -q $(linter_dir).zip -d $(linter_dir)
	$(move) $(linter_dir)\golangci-lint-$(GOLANGCI_LINT_VERSION)-windows-amd64\golangci-lint.exe $(linter)
else
	mkdir -p $(linter_dir)
ifeq ($(uname),Linux)
	curl -LsS https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_LINT_VERSION)/golangci-lint-$(GOLANGCI_LINT_VERSION)-linux-amd64.tar.gz | tar zxv --strip=1 -C $(linter_dir)
else
	curl -LsS https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_LINT_VERSION)/golangci-lint-$(GOLANGCI_LINT_VERSION)-darwin-amd64.tar.gz | tar zxv --strip=1 -C $(linter_dir)
endif

endif

# hugo
hugo_dir=$(TOOLS_DIR)$(slash)hugo-$(HUGO_VERSION)
hugo=$(hugo_dir)$(slash)hugo$(exe_suffix)

$(hugo):
	@echo Downloading Hugo v$(HUGO_VERSION) to $(hugo)
	$(mkdir) $(TOOLS_DIR)$(slash)hugo-$(HUGO_VERSION)

ifeq ($(uname),Windows)
	curl -LsS -o $(hugo_dir).zip https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_Windows-64bit.zip
	unzip -q $(hugo_dir).zip -d $(hugo_dir)
else

ifeq ($(uname),Linux)
	curl -LsS https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_Linux-64bit.tar.gz | tar zxv -C $(hugo_dir)
else
	curl -LsS https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_macOS-64bit.tar.gz | tar zxv -C $(hugo_dir)
endif

endif

# goreleaser
goreleaser_dir=$(TOOLS_DIR)$(slash)goreleaser-$(GORELEASER_VERSION)
goreleaser=$(goreleaser_dir)$(slash)goreleaser$(exe_suffix)

$(goreleaser):
	@echo Downloading GoReleaser $(GORELEASER_VERSION) to $(goreleaser)
	-$(mkdir) $(goreleaser_dir)
ifeq ($(uname),Windows)
	curl -LsS -o $(goreleaser_dir).zip https://github.com/goreleaser/goreleaser/releases/download/$(GORELEASER_VERSION)/goreleaser_Windows_x86_64.zip
	unzip -q $(goreleaser_dir).zip -d $(goreleaser_dir)
else
	curl -LsS https://github.com/goreleaser/goreleaser/releases/download/$(GORELEASER_VERSION)/goreleaser_$$(uname -s)_$$(uname -m).tar.gz | tar zx -C $(TOOLS_DIR)/goreleaser-$(GORELEASER_VERSION)
endif

ifeq ($(TRAVIS_PULL_REQUEST),false)

ifneq ($(TRAVIS_TAG),)
# travis, tagged release
KOPIA_VERSION:=$(TRAVIS_TAG:v%=%)
else
# travis, non-tagged release
KOPIA_VERSION:=$(commit_date_ymd).0.$(commit_time_of_day)
endif

else

# non-travis, or travis PR
KOPIA_VERSION:=$(date_ymd).0.0-$(shell git rev-parse --short HEAD)

endif

# embedded in the HTML pages
export REACT_APP_SHORT_VERSION_INFO:=$(KOPIA_VERSION)
export REACT_APP_FULL_VERSION_INFO:=$(KOPIA_VERSION) built on $(date_full) $(shell hostname)

clean-tools:
	rm -rf $(TOOLS_DIR)

windows_signing_dir=$(TOOLS_DIR)$(slash)win_signing

windows-signing-tools:
ifeq ($(TRAVIS_OS_NAME),windows)
ifneq ($(WINDOWS_SIGNING_TOOLS_URL),)
	echo Installing Windows signing tools to $(windows_signing_dir)...
	-$(mkdir) $(windows_signing_dir)
	curl -LsS -o $(windows_signing_dir).zip $(WINDOWS_SIGNING_TOOLS_URL)
	unzip -a -q $(windows_signing_dir).zip -d $(windows_signing_dir)
	powershell -noprofile -executionpolicy bypass $(windows_signing_dir)\\setup.ps1
else
	echo Not installing Windows signing tools because WINDOWS_SIGNING_TOOLS_URL is not set
endif
endif

all-tools: $(npm) $(goreleaser) $(linter) $(hugo) $(go_bindata) windows-signing-tools

