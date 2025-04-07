SHELL:=/bin/bash

include ../tools/tools.mk

deps: node_modules/.up-to-date

node_modules/.up-to-date: $(npm) package.json package-lock.json
	$(retry) $(npm) $(npm_flags) $(npm_install_or_ci) --no-audit
	$(npm) $(npm_flags) audit --omit=dev
	echo updated > node_modules/.up-to-date

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

# Running as part of a pull_request event on GH, or outside a GH workflow, then
# don't build installer and don't publish
ifneq ($(FORCE_KOPIA_UI_SIGN),)
electron_builder_flags+=--dir

else

# unset CSC_LINK completely, otherwise electron builder hangs attempting to
# notarize.
unexport CSC_LINK
unexport CSC_KEY_PASSWORD
unexport KOPIA_UI_NOTARIZE

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
ifeq ($(KOPIA_UI_CURRENT_ARCH_ONLY),)
electron_builder_flags+=--x64 --arm64
endif
endif

# build x86_64 and arm64 binaries
ifeq ($(GOOS),linux)
ifeq ($(KOPIA_UI_CURRENT_ARCH_ONLY),)
electron_builder_flags+=--x64 --arm64 --armv7l
endif
endif

dev: node_modules/.up-to-date
	$(npm) $(npm_flags) run dev

run:
	$(npm) $(npm_flags) run start-electron-prebuilt

e2e-test:
	$(npm) $(npm_flags) run e2e

build-electron: ../dist/kopia-ui/.up-to-date

# rebuild packages if HTML, embedded EXE or build config changed.
../dist/kopia-ui/.up-to-date: package.json ../dist/kopia_*/kopia* node_modules/.up-to-date public/* resources/*/*/*
	$(retry) $(npm) $(npm_flags) run build-electron -- $(electron_builder_flags) -p $(electron_publish_flag)
	echo updated > ../dist/kopia-ui/.up-to-date
