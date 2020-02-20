COVERAGE_PACKAGES=github.com/kopia/kopia/repo/...,github.com/kopia/kopia/fs/...,github.com/kopia/kopia/snapshot/...
GO_TEST=go test
PARALLEL=8
TEST_FLAGS=
KOPIA_INTEGRATION_EXE=$(CURDIR)/dist/integration/kopia.exe

all: test lint vet integration-tests

include tools/tools.mk

-include ./Makefile.local.mk

install: html-ui-bindata
	go install -tags embedhtml

quick-install:
	# same as install but assumes HTMLUI has been built
	go install -tags embedhtml

install-noui: 
	go install

escape-analysis:
	go build -gcflags '-m -l' github.com/kopia/kopia/...

clean:
	make clean-tools
	make -C htmlui clean
	rm -rf dist/ internal/server/htmlui_bindata.go

play:
	go run cmd/playground/main.go

lint: $(linter)
	$(linter) --deadline 180s run $(linter_flags)

lint-and-log: $(linter)
	$(linter) --deadline 180s run $(linter_flags) | tee .linterr.txt

vet:
	go vet -all .

travis-setup: travis-install-gpg-key travis-install-test-credentials all-tools
	go mod download
	make -C htmlui node_modules
	make -C app node_modules

website:
	$(MAKE) -C site build

html-ui:
	$(MAKE) -C htmlui build-html CI=true

html-ui-tests:
	$(MAKE) -C htmlui test CI=true

html-ui-bindata: html-ui $(go_bindata)
	(cd htmlui/build && $(go_bindata) -fs -tags embedhtml -o "$(CURDIR)/internal/server/htmlui_bindata.go" -pkg server -ignore '.map' . static/css static/js static/media)

html-ui-bindata-fallback: $(go_bindata)
	(cd internal/server && $(go_bindata) -fs -tags !embedhtml -o "$(CURDIR)/internal/server/htmlui_fallback.go" -pkg server index.html)

kopia-ui: goreleaser
	$(MAKE) -C app build-electron

ifeq ($(TRAVIS_OS_NAME),windows)
travis-release: install kopia-ui
	$(MAKE) lint test html-ui-tests
	$(MAKE) integration-tests
endif

ifeq ($(TRAVIS_OS_NAME),osx)
travis-release: install kopia-ui
	$(MAKE) lint test html-ui-tests
	$(MAKE) integration-tests
endif

ifeq ($(TRAVIS_OS_NAME),linux)
travis-release: goreleaser kopia-ui website
	$(MAKE) test-all
	$(MAKE) integration-tests
	$(MAKE) stress-test
ifneq ($(TRAVIS_TAG),)
	$(MAKE) travis-create-long-term-repository
endif
endif

test-all: lint vet test-with-coverage html-ui-tests html-ui-tests

# goreleaser - builds binaries for all platforms
GORELEASER_OPTIONS=--rm-dist --skip-publish --parallelism=6

sign_gpg=1
ifneq ($(TRAVIS_PULL_REQUEST),false)
	# not running on travis, or travis in PR mode, skip signing
	sign_gpg=0
endif

ifeq ($(TRAVIS_OS_NAME),windows)
	# signing does not work on Windows on Travis
	sign_gpg=0
endif

ifeq ($(sign_gpg),0)
GORELEASER_OPTIONS+=--skip-sign
endif

ifeq ($(TRAVIS_TAG),)
	# not a tagged release
	GORELEASER_OPTIONS+=--snapshot
endif

goreleaser: $(goreleaser)
	$(goreleaser) release $(GORELEASER_OPTIONS)

ifeq ($(TRAVIS_PULL_REQUEST),false)

upload-coverage: $(GOVERALLS_TOOL) test-with-coverage
	$(GOVERALLS_TOOL) -service=travis-ci -coverprofile=tmp.cov

else

uload-coverage:
	@echo Not uploading coverage during PR build.

endif

dev-deps:
	GO111MODULE=off go get -u golang.org/x/tools/cmd/gorename
	GO111MODULE=off go get -u golang.org/x/tools/cmd/guru
	GO111MODULE=off go get -u github.com/nsf/gocode
	GO111MODULE=off go get -u github.com/rogpeppe/godef
	GO111MODULE=off go get -u github.com/lukehoban/go-outline
	GO111MODULE=off go get -u github.com/newhook/go-symbols
	GO111MODULE=off go get -u github.com/sqs/goreturns
	
test-with-coverage:
	$(GO_TEST) -count=1 -coverprofile=tmp.cov --coverpkg $(COVERAGE_PACKAGES) -timeout 90s `go list ./...`

test-with-coverage-pkgonly:
	$(GO_TEST) -count=1 -coverprofile=tmp.cov -timeout 90s github.com/kopia/kopia/...

test:
	$(GO_TEST) -count=1 -timeout 90s github.com/kopia/kopia/...

vtest:
	$(GO_TEST) -count=1 -short -v -timeout 90s github.com/kopia/kopia/...

dist-binary:
	go build -o $(KOPIA_INTEGRATION_EXE) github.com/kopia/kopia

integration-tests: dist-binary
	KOPIA_EXE=$(KOPIA_INTEGRATION_EXE) $(GO_TEST) $(TEST_FLAGS) -count=1 -parallel $(PARALLEL) -timeout 300s github.com/kopia/kopia/tests/end_to_end_test

stress-test:
	KOPIA_LONG_STRESS_TEST=1 $(GO_TEST) -count=1 -timeout 200s github.com/kopia/kopia/tests/stress_test
	$(GO_TEST) -count=1 -timeout 200s github.com/kopia/kopia/tests/repository_stress_test

godoc:
	godoc -http=:33333

coverage: test-with-coverage coverage-html

coverage-html:
	go tool cover -html=tmp.cov

official-release:
	git tag $(RELEASE_VERSION) -m $(RELEASE_VERSION)
	git push -u upstream $(RELEASE_VERSION)

goreturns:
	find . -name '*.go' | xargs goreturns -w --local github.com/kopia/kopia

# this indicates we're running on Travis CI and NOT processing pull request.
ifeq ($(TRAVIS_PULL_REQUEST),false)

travis-install-gpg-key:
ifeq ($(TRAVIS_OS_NAME),windows)
	# https://travis-ci.community/t/windows-build-timeout-after-success-ps-shows-gpg-agent/4967/4
	@echo Not installing GPG key on Windows...
else
	@echo Installing GPG key...
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in kopia.gpg.enc -out /tmp/kopia.gpg -d
	gpg --import /tmp/kopia.gpg
endif

travis-install-test-credentials:
	@echo Installing test credentials...
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in tests/credentials/gcs/test_service_account.json.enc -out repo/blob/gcs/test_service_account.json -d
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in tests/credentials/sftp/id_kopia.enc -out repo/blob/sftp/id_kopia -d
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in tests/credentials/sftp/known_hosts.enc -out repo/blob/sftp/known_hosts -d

travis-install-cloud-sdk: travis-install-test-credentials
	if [ ! -d $(HOME)/google-cloud-sdk ]; then curl https://sdk.cloud.google.com | CLOUDSDK_CORE_DISABLE_PROMPTS=1 bash; fi
	$(HOME)/google-cloud-sdk/bin/gcloud auth activate-service-account --key-file repo/blob/gcs/test_service_account.json

else

travis-install-gpg-key:
	@echo Not installing GPG key.

travis-install-test-credentials:
	@echo Not installing test credentials.

travis-install-cloud-sdk:
	@echo Not installing Cloud SDK.

endif

ifneq ($(TRAVIS_TAG),)

travis-create-long-term-repository: dist-binary travis-install-cloud-sdk
	echo Creating long-term repository $(TRAVIS_TAG)...
	KOPIA_EXE=$(KOPIA_INTEGRATION_EXE) ./tests/compat_test/gen-compat-repo.sh

else

travis-create-long-term-repository:
	echo Not creating long-term repository.

endif
