SHELL:=/bin/bash
REPO_OWNER=kopia

electron_builder_flags:=
electron_publish_flag:=never

ifeq ($(TRAVIS_PULL_REQUEST),false)

electron_publish_flag=always

ifneq ($(TRAVIS_TAG),)
# tagged release - create draft release, but don't publish
electron_builder_flags+=-c.extraMetadata.version=$(TRAVIS_TAG:v%=%)
electron_builder_flags+=-c.publish.releaseType=draft
electron_builder_flags+=-c.publish.owner=$(REPO_OWNER)
electron_builder_flags+=-c.publish.repo=kopia
else
# post-submit run, create a release in another repo
electron_builder_flags+=-c.extraMetadata.version=$(shell date +%y%m%d%H%M.$(TRAVIS_BUILD_NUMBER).0)-pre-$(shell echo $(TRAVIS_COMMIT) | cut -b 1-8)
electron_builder_flags+=-c.publish.owner=$(REPO_OWNER)
electron_builder_flags+=-c.publish.repo=kopia-ui-release
electron_builder_flags+=-c.publish.releaseType=release
endif

else

# not running on Travis, or Travis in PR mode, don't build installer and don't publish
electron_builder_flags+=--dir

endif

ifeq ($(TRAVIS_OS_NAME),windows)
# disable Kopia UI code signing on Windows.
undefine CSC_LINK
undefine CSC_KEY_PASSWORD
endif

ifeq ($(TRAVIS_OS_NAME),linux)
# no packaging on Linux yet.
electron_builder_flags+=--dir
electron_publish_flag=never
endif

dev: node_modules
	$(npm) $(npm_flags) run dev

run: build-html
	$(npm) $(npm_flags) run start-electron-prebuilt

build-html: node_modules
	$(npm) $(npm_flags) run build-html

build-electron: node_modules build-html
	$(npm) $(npm_flags) run build-electron -- $(electron_builder_flags) -p $(electron_publish_flag)

include ../tools/tools.mk

node_modules: $(npm)
	$(npm) $(npm_flags) install
