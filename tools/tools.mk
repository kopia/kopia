SELF_DIR := $(realpath $(dir $(lastword $(MAKEFILE_LIST))))
TOOLS_DIR:=$(SELF_DIR)/.tools
uname := $(shell uname -s)

# tool versions
GOLANGCI_LINT_VERSION=v1.22.2
NODE_VERSION=12.13.0
HUGO_VERSION=0.59.1
GORELEASER_VERSION=v0.125.0

# goveralls
GOVERALLS_TOOL=$(TOOLS_DIR)/bin/goveralls

$(GOVERALLS_TOOL):
	mkdir -p $(TOOLS_DIR)
	GO111MODULE=off GOPATH=$(TOOLS_DIR) go get github.com/mattn/goveralls

# nodejs / npm
NPM_TOOL=$(TOOLS_DIR)/nodejs/node/bin/npm
TOOL_PATH=$(PATH):$(TOOLS_DIR)/nodejs/node/bin

$(NPM_TOOL):
	echo SELF_DIR: $(SELF_DIR)
	mkdir -p $(TOOLS_DIR)/nodejs

ifeq ($(uname),Linux)
	curl -LsS https://nodejs.org/dist/v$(NODE_VERSION)/node-v$(NODE_VERSION)-linux-x64.tar.gz | tar zx -C $(TOOLS_DIR)/nodejs
else
	curl -LsS https://nodejs.org/dist/v$(NODE_VERSION)/node-v$(NODE_VERSION)-darwin-x64.tar.gz | tar zx -C $(TOOLS_DIR)/nodejs
endif
	mv $(TOOLS_DIR)/nodejs/node-v$(NODE_VERSION)* $(TOOLS_DIR)/nodejs/node/

BINDATA_TOOL=$(TOOLS_DIR)/bin/go-bindata

$(BINDATA_TOOL):
	go build -o $(BINDATA_TOOL) github.com/go-bindata/go-bindata/go-bindata

# linter
LINTER_TOOL=$(TOOLS_DIR)/bin/golangci-lint-$(GOLANGCI_LINT_VERSION)

$(LINTER_TOOL):
	mkdir -p $(TOOLS_DIR)
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(TOOLS_DIR)/bin/ $(GOLANGCI_LINT_VERSION)
	ln -sf $(TOOLS_DIR)/bin/golangci-lint $(LINTER_TOOL)

# hugo
HUGO_TOOL=$(TOOLS_DIR)/hugo/hugo

$(HUGO_TOOL):
	mkdir -p $(TOOLS_DIR)/hugo

ifeq ($(uname),Linux)
	curl -LsS https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_Linux-64bit.tar.gz | tar zxv -C $(TOOLS_DIR)/hugo
else
	curl -LsS https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_macOS-64bit.tar.gz | tar zxv -C $(TOOLS_DIR)/hugo
endif

# linter
GORELEASER_TOOL=$(TOOLS_DIR)/goreleaser-$(GORELEASER_VERSION)/goreleaser

$(GORELEASER_TOOL):
	mkdir -p $(TOOLS_DIR)/goreleaser-$(GORELEASER_VERSION)
	curl -LsS https://github.com/goreleaser/goreleaser/releases/download/$(GORELEASER_VERSION)/goreleaser_$$(uname -s)_$$(uname -m).tar.gz | tar zx -C $(TOOLS_DIR)/goreleaser-$(GORELEASER_VERSION)

clean-tools:
	rm -rf $(TOOLS_DIR)