HUGO_VERSION=0.55.5
NODE_VERSION=10.15.3
# don't put tools under current directory, otherwise 'make server' fails because there are too
# many open files due to 'node_modules'
TOOLS_DIR=$(CURDIR)/../.tools
NPM_TOOL=$(TOOLS_DIR)/nodejs/node/bin/npm
WATCH=true

HUGO_TOOL=$(TOOLS_DIR)/hugo/hugo

uname := $(shell uname -s)

build: gen-cli-reference-pages $(HUGO_TOOL)
	$(HUGO_TOOL)

server: $(HUGO_TOOL)
	$(HUGO_TOOL) server --watch=$(WATCH)

$(HUGO_TOOL):
	make install-tools

$(NPM_TOOL):
	make install-tools

clean:
	rm -rf public/ resources/ node_modules/ $(TOOLS_DIR)/

install-tools:
	mkdir -p $(TOOLS_DIR)/hugo
	mkdir -p $(TOOLS_DIR)/nodejs

ifeq ($(uname),Linux)
	curl -LsS https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_Linux-64bit.tar.gz | tar zxv -C $(TOOLS_DIR)/hugo
	curl -LsS https://nodejs.org/dist/v$(NODE_VERSION)/node-v$(NODE_VERSION)-linux-x64.tar.gz | tar zx -C $(TOOLS_DIR)/nodejs
else
	curl -LsS https://github.com/gohugoio/hugo/releases/download/v$(HUGO_VERSION)/hugo_extended_$(HUGO_VERSION)_macOS-64bit.tar.gz | tar zxv -C $(TOOLS_DIR)/hugo
	curl -LsS https://nodejs.org/dist/v$(NODE_VERSION)/node-v$(NODE_VERSION)-darwin-x64.tar.gz | tar zx -C $(TOOLS_DIR)/nodejs
endif
	mv $(TOOLS_DIR)/nodejs/node-v$(NODE_VERSION)* $(TOOLS_DIR)/nodejs/node/
	$(NPM_TOOL) install

gen-cli-reference-pages:
	rm -rf content/docs/Reference/Command-Line/Advanced
	rm -rf content/docs/Reference/Command-Line/Common
	go run github.com/kopia/kopia/site/cli2md
