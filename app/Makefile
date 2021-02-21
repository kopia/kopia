SHELL:=/bin/bash

include ../tools/tools.mk

node_modules: $(npm)
	$(npm) $(npm_flags) install

electron_builder_flags:=
electron_builder_flags+=-c.extraMetadata.version=$(KOPIA_VERSION:v%=%)
electron_publish_flag:=never

ifeq ($(PUBLISH_BINARIES)/$(IS_PULL_REQUEST),true/false)

electron_publish_flag=always

ifneq ($(CI_TAG),)
# tagged release - create draft release, but don't publish
electron_builder_flags+=-c.publish.releaseType=release
electron_builder_flags+=-c.publish.owner=$(REPO_OWNER)
electron_builder_flags+=-c.publish.repo=kopia
else
# post-submit run, create a release in another repo
electron_builder_flags+=-c.publish.owner=$(REPO_OWNER)
electron_builder_flags+=-c.publish.repo=kopia-ui-release
electron_builder_flags+=-c.publish.releaseType=release
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

dev: node_modules
	$(npm) $(npm_flags) run dev

run: build-html
	$(npm) $(npm_flags) run start-electron-prebuilt

build-html: node_modules
	$(npm) $(npm_flags) run build-html

build-electron: node_modules build-html
	$(npm) $(npm_flags) run build-electron -- $(electron_builder_flags) -p $(electron_publish_flag)
