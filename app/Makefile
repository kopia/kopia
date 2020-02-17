SHELL:=/bin/bash

dev: node_modules
	$(npm) $(npm_flags) run dev

run: build-html
	$(npm) $(npm_flags) run start-electron-prebuilt

build-html: node_modules
	$(npm) $(npm_flags) run build-html

build-electron-dir: node_modules build-html
	$(npm) $(npm_flags) run build-electron-dir

build-electron: node_modules build-html
	$(npm) $(npm_flags) run build-electron

include ../tools/tools.mk

node_modules: $(npm)
	$(npm) $(npm_flags) install
