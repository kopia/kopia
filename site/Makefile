# don't put tools under current directory, otherwise 'make server' fails because there are too
# many open files due to 'node_modules'
WATCH=true

all: build

include ../tools/tools.mk

build: gen-cli-reference-pages $(HUGO_TOOL) node_modules
	PATH=$(TOOL_PATH) $(HUGO_TOOL)

server: $(HUGO_TOOL)
	PATH=$(TOOL_PATH) $(HUGO_TOOL) server --watch=$(WATCH)

node_modules: $(NPM_TOOL)
	PATH=$(TOOL_PATH) $(NPM_TOOL) install

clean:
	rm -rf public/ resources/ node_modules/ $(TOOLS_DIR)/

gen-cli-reference-pages:
	rm -rf content/docs/Reference/Command-Line/Advanced
	rm -rf content/docs/Reference/Command-Line/Common
	go run github.com/kopia/kopia/site/cli2md
