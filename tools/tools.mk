# to build on Windows you need the following basic Unix tools in your PATH installed using Chocolatey
#
# make
# unzip
# curl

# uname will be Windows, Darwin, Linux
ifeq ($(OS),Windows_NT)
	exe_suffix := .exe
	cmd_suffix := .cmd
	uname := Windows
	slash=\\
	path_separator=;
ifeq ($(TRAVIS_OS_NAME),windows)
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
endif

SELF_DIR := $(subst /,$(slash),$(realpath $(dir $(lastword $(MAKEFILE_LIST)))))
TOOLS_DIR:=$(SELF_DIR)$(slash).tools

# tool versions
GOLANGCI_LINT_VERSION=1.23.6
NODE_VERSION=12.13.0
HUGO_VERSION=0.59.1
GORELEASER_VERSION=v0.125.0

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
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(linter_dir) v$(GOLANGCI_LINT_VERSION)
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

clean-tools:
	rm -rf $(TOOLS_DIR)

all-tools: $(npm) $(goreleaser) $(linter) $(hugo) $(go_bindata)
