dev: node_modules
	PATH=$(TOOL_PATH) $(NPM_TOOL) run dev

run: build-html
	PATH=$(TOOL_PATH) $(NPM_TOOL) run start-electron-prebuilt

build-html: node_modules
	PATH=$(TOOL_PATH) $(NPM_TOOL) run build-html

build-electron-dir: node_modules build-html
	PATH=$(TOOL_PATH) $(NPM_TOOL) run build-electron-dir

build-all-mac: node_modules build-html
	PATH=$(TOOL_PATH) $(NPM_TOOL) run build-electron-mac

build-all-win-linux-docker:
	docker run --rm -ti \
		--env ELECTRON_CACHE="/root/.cache/electron" \
		--env ELECTRON_BUILDER_CACHE="/root/.cache/electron-builder" \
		-v $(CURDIR):/project \
		-v ~/.cache/electron:/root/.cache/electron \
		-v $(CURDIR)/../dist:/dist \
		-v ~/.cache/electron-builder:/root/.cache/electron-builder \
		electronuserland/builder:wine \
		/bin/bash -c "npm install && npm run build-html && npm run build-electron-win-linux"

include ../tools/tools.mk

node_modules: $(NPM_TOOL)
	PATH=$(TOOL_PATH) $(NPM_TOOL) install
