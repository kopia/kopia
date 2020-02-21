SHELL:=/bin/bash
REPO_OWNER=kopia

electron_builder_flags:=
electron_publish_flag:=never
electron_builder_env:=

ifeq ($(TRAVIS_PULL_REQUEST),false)

ifneq ($(TRAVIS_TAG),)
# tagged release - create draft release, but don't publish
electron_publish_flag=always
electron_builder_flags+=-c.extraMetadata.version=$(TRAVIS_TAG:v%=%)
electron_builder_flags+=-c.publish.releaseType=draft
electron_builder_flags+=-c.publish.owner=$(REPO_OWNER)
electron_builder_flags+=-c.publish.repo=kopia
else
# post-submit run, create a release in another repo
electron_publish_flag=always
electron_builder_flags+=-c.extraMetadata.version=$(shell date +%Y%m%d).0.$(TRAVIS_BUILD_NUMBER)-testing
electron_builder_flags+=-c.publish.owner=$(REPO_OWNER)
electron_builder_flags+=-c.publish.repo=kopia-ui-release
electron_builder_flags+=-c.publish.releaseType=release
endif

else

ifneq ($(TRAVIS_PULL_REQUEST),)

electron_builder_env=PUBLISH_FOR_PULL_REQUEST=true

# travis PR run
electron_publish_flag=always
electron_builder_flags+=-c.extraMetadata.version=$(shell date +%Y%m%d).0.$(TRAVIS_BUILD_NUMBER)-pr$(TRAVIS_PULL_REQUEST)
electron_builder_flags+=-c.publish.owner=$(REPO_OWNER)
electron_builder_flags+=-c.publish.repo=kopia-ui-pr
electron_builder_flags+=-c.publish.releaseType=release
else

# not running on Travis, don't build installer and don't publish
electron_publish_flag=never
electron_builder_flags+=--dir

endif

endif

ifeq ($(TRAVIS_OS_NAME),windows)
# disable Kopia UI code signing on Windows.
undefine CSC_LINK
undefine CSC_KEY_PASSWORD
endif

dev: node_modules
	$(npm) $(npm_flags) run dev

run: build-html
	$(npm) $(npm_flags) run start-electron-prebuilt

build-html: node_modules
	$(npm) $(npm_flags) run build-html

build-electron: node_modules build-html
	$(electron_builder_env) $(npm) $(npm_flags) run build-electron -- $(electron_builder_flags) -p $(electron_publish_flag)

include ../tools/tools.mk

node_modules: $(npm)
	$(npm) $(npm_flags) install
