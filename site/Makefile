HUGO_VERSION=0.59.1
# don't put tools under current directory, otherwise 'make server' fails because there are too
# many open files due to 'node_modules'
TOOLS_DIR=$(CURDIR)/../.tools
NPM_TOOL=$(TOOLS_DIR)/nodejs/node/bin/npm
WATCH=true
TOOL_PATH=$(PATH):$(TOOLS_DIR)/nodejs/node/bin

HUGO_TOOL=$(TOOLS_DIR)/hugo/hugo

uname := $(shell uname -s)

build: gen-cli-reference-pages $(HUGO_TOOL) module-deps
	PATH=$(TOOL_PATH) $(HUGO_TOOL)

server: $(HUGO_TOOL)
	PATH=$(TOOL_PATH) $(HUGO_TOOL) server --watch=$(WATCH)

module-deps: node_modules

node_modules: $(NPM_TOOL)
	PATH=$(TOOL_PATH) $(NPM_TOOL) install

$(HUGO_TOOL):
	make install-tools

$(NPM_TOOL):
	make -C .. install-webtools

clean:
	rm -rf public/ resources/ node_modules/ $(TOOLS_DIR)/

install-tools:
	mkdir -p $(TOOLS_DIR)/hugo

ifeq ($(uname),Linux)
	curl -LsS https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_Linux-64bit.tar.gz | tar zxv -C $(TOOLS_DIR)/hugo
else
	curl -LsS https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_macOS-64bit.tar.gz | tar zxv -C $(TOOLS_DIR)/hugo
endif

gen-cli-reference-pages:
	rm -rf content/docs/Reference/Command-Line/Advanced
	rm -rf content/docs/Reference/Command-Line/Common
	go run github.com/kopia/kopia/site/cli2md
