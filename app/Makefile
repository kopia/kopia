SHELL:=/bin/bash

include ../tools/tools.mk

deps: node_modules/.up-to-date

node_modules/.up-to-date: $(npm) package.json package-lock.json
	$(retry) $(npm) $(npm_flags) install
	echo updated > node_modules/.up-to-date

electron_builder_flags:=
electron_builder_flags+=-c.extraMetadata.version=$(KOPIA_VERSION:v%=%)

electron_publish_flag:=never

ifeq ($(IS_PULL_REQUEST),false)

electron_builder_flags+=-c.publish.owner=$(REPO_OWNER)

ifeq ($(CI_TAG),)
ifneq ($(NON_TAG_RELEASE_REPO),)
electron_builder_flags+=-c.publish.repo=$(NON_TAG_RELEASE_REPO)
endif
endif

else

# not running on Travis, or Travis in PR mode, don't build installer and don't publish
ifneq ($(FORCE_KOPIA_UI_SIGN),)
electron_builder_flags+=--dir
endif

endif

# empty CSC_LINK, unset completely since empty value confuses electron builder.
ifeq ($(CSC_LINK),)

unexport CSC_LINK
unexport CSC_KEY_PASSWORD

endif

ifeq ($(GOOS),windows)

# disable Kopia UI code signing on Windows.
unexport CSC_LINK
unexport CSC_KEY_PASSWORD

endif

# build x86_64 and apple silicon binaries
ifeq ($(GOOS),darwin)
electron_builder_flags+=--x64 --arm64
endif

dev: node_modules/.up-to-date
	$(npm) $(npm_flags) run dev

run: build-html
	$(npm) $(npm_flags) run start-electron-prebuilt

build-html: build/index.html

build/index.html: node_modules/.up-to-date src/* public/* resources/*/*/*
	$(npm) $(npm_flags) run build-html

build-electron: ../dist/kopia-ui/.up-to-date

# rebuild packages if HTML, embedded EXE or build config changed.
../dist/kopia-ui/.up-to-date: build/index.html package.json ../dist/kopia_*/kopia*
	$(retry) $(npm) $(npm_flags) run build-electron -- $(electron_builder_flags) -p $(electron_publish_flag)
	echo updated > ../dist/kopia-ui/.up-to-date
