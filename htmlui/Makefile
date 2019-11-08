all:

include ../tools/tools.mk

dev: node_modules
	PATH=$(TOOL_PATH) $(NPM_TOOL) run start

build-html: node_modules
	PATH=$(TOOL_PATH) $(NPM_TOOL) run build

node_modules: $(NPM_TOOL)
	PATH=$(TOOL_PATH) $(NPM_TOOL) install
