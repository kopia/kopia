# don't put tools under current directory, otherwise 'make server' fails because there are too
# many open files due to 'node_modules'
WATCH=false

all: build

include ../tools/tools.mk

ifeq ($(NETLIFY)/$(CONTEXT),true/production)
export HUGO_ENV=production
endif

install-tools:
	make -C .. $(npm) $(cli2mdbin) $(hugo)

build: install-tools gen-cli-reference-pages node_modules
	$(hugo)

server: install-tools
	$(hugo) server --watch=$(WATCH)

node_modules: install-tools
	$(npm) $(npm_flags) $(npm_install_or_ci) --no-audit
	$(npm) $(npm_flags) audit --omit=dev

clean:
	rm -rf public/ resources/ node_modules/ $(TOOLS_DIR)/

gen-cli-reference-pages: install-tools
	$(cli2mdbin)
