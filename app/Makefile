dev: node_modules
	PATH=$(TOOL_PATH) $(NPM_TOOL) run dev

run: build-html
	PATH=$(TOOL_PATH) $(NPM_TOOL) run start-electron-prebuilt

build-html: node_modules
	PATH=$(TOOL_PATH) $(NPM_TOOL) run build-html

build-electron-win: node_modules build-html
	PATH=$(TOOL_PATH) $(NPM_TOOL) run build-electron-win

build-electron-linux: node_modules build-html
	PATH=$(TOOL_PATH) $(NPM_TOOL) run build-electron-linux

build-electron-mac: node_modules build-html
	PATH=$(TOOL_PATH) $(NPM_TOOL) run build-electron-mac

build-electron-all: node_modules build-html
	PATH=$(TOOL_PATH) $(NPM_TOOL) run build-electron-all

build-electron-dir: node_modules build-html
	PATH=$(TOOL_PATH) $(NPM_TOOL) run build-electron-dir

include ../tools/tools.mk

node_modules: $(NPM_TOOL)
	PATH=$(TOOL_PATH) $(NPM_TOOL) install
