# don't put tools under current directory, otherwise 'make server' fails because there are too
# many open files due to 'node_modules'
WATCH=false

all: build

include ../tools/tools.mk

build: gen-cli-reference-pages $(hugo) node_modules
	$(hugo)

server: $(hugo)
	$(hugo) server --watch=$(WATCH)

node_modules: $(npm)
	$(npm) $(npm_flags) install

clean:
	rm -rf public/ resources/ node_modules/ $(TOOLS_DIR)/

gen-cli-reference-pages:
ifeq ($(OS),Windows_NT)
	-rd /q /s content\\docs\\Reference\\Command-Line\\Advanced
	-rd /q /s content\\docs\\Reference\\Command-Line\\Common
else
	rm -rf content/docs/Reference/Command-Line/Advanced
	rm -rf content/docs/Reference/Command-Line/Common
endif
	go run github.com/kopia/kopia/site/cli2md
