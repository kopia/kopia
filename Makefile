COVERAGE_PACKAGES=github.com/kopia/kopia/repo/...,github.com/kopia/kopia/fs/...,github.com/kopia/kopia/snapshot/...
TEST_FLAGS?=

KOPIA_INTEGRATION_EXE=$(CURDIR)/dist/testing_$(GOOS)_$(GOARCH)/kopia.exe
TESTING_ACTION_EXE=$(CURDIR)/dist/testing_$(GOOS)_$(GOARCH)/testingaction.exe
FIO_DOCKER_TAG=ljishen/fio
REPEAT_TEST=1

export BOTO_PATH=$(CURDIR)/tools/.boto

# get a list all go files
rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) $(filter $(subst *,%,$2),$d))
go_source_dirs=cli fs internal repo snapshot
all_go_sources=$(foreach d,$(go_source_dirs),$(call rwildcard,$d/,*.go)) $(wildcard *.go)

all: test lint vet integration-tests

include tools/tools.mk

kopia_ui_embedded_exe=dist/kopia_$(GOOS)_$(GOARCH)/kopia$(exe_suffix)

ifeq ($(GOOS),darwin)
	# on macOS, Kopia uses universal binary that works for AMD64 and ARM64
	kopia_ui_embedded_exe=dist/kopia_darwin_universal/kopia
endif

ifeq ($(GOARCH),arm)
	kopia_ui_embedded_exe=dist/kopia_linux_arm_6/kopia
endif

GOTESTSUM_FORMAT=pkgname-and-test-fails
GOTESTSUM_FLAGS=--format=$(GOTESTSUM_FORMAT) --no-summary=skipped 
GO_TEST=$(gotestsum) $(GOTESTSUM_FLAGS) --

LINTER_DEADLINE=300s
UNIT_TESTS_TIMEOUT=300s

ifeq ($(GOARCH),amd64)
PARALLEL=8
else
# tweaks for less powerful platforms
PARALLEL=2
endif

-include ./Makefile.local.mk

install: html-ui
	go install $(KOPIA_BUILD_FLAGS) -tags embedhtml

install-noui:
	go install $(KOPIA_BUILD_FLAGS)

lint: $(linter)
	$(linter) --deadline $(LINTER_DEADLINE) run $(linter_flags)

lint-and-log: $(linter)
	$(linter) --deadline $(LINTER_DEADLINE) run $(linter_flags) | tee .linterr.txt


vet:
	go vet -all .

go-modules:
	go mod download

app-node-modules: $(npm)
ifeq ($(GOARCH),amd64)
	make -C app deps
endif

htmlui-node-modules: $(npm)
	make -C htmlui deps

ci-setup: ci-credentials go-modules all-tools htmlui-node-modules app-node-modules
ifeq ($(CI),true)
	-git checkout go.mod go.sum
endif

website:
	$(MAKE) -C site build

html-ui: htmlui-node-modules
	$(MAKE) -C htmlui build-html CI=true

html-ui-tests: htmlui-node-modules
	$(MAKE) -C htmlui test CI=true

kopia-ui: $(kopia_ui_embedded_exe)
	$(MAKE) -C app build-electron

# build-current-os-noui compiles a binary for the current os/arch in the same location as goreleaser
# kopia-ui build needs this particular location to embed the correct server binary.
# note we're not building or embedding HTML UI to speed up PR testing process.
build-current-os-noui:
	go build $(KOPIA_BUILD_FLAGS) -o $(kopia_ui_embedded_exe)

# build HTML UI files to be embedded in Kopia binary.
htmlui/build/index.html: html-ui

# on macOS build and sign AMD64, ARM64 and Universal binary and *.tar.gz files for them
dist/kopia_darwin_universal/kopia dist/kopia_darwin_amd64/kopia dist/kopia_darwin_arm6/kopia: htmlui/build/index.html $(all_go_sources)
	$(MAKE) signing-tools
	GOARCH=arm64 go build $(KOPIA_BUILD_FLAGS) -o dist/kopia_darwin_arm64/kopia -tags embedhtml
	GOARCH=amd64 go build $(KOPIA_BUILD_FLAGS) -o dist/kopia_darwin_amd64/kopia -tags embedhtml
	mkdir -p dist/kopia_darwin_universal
	lipo -create -output dist/kopia_darwin_universal/kopia dist/kopia_darwin_arm64/kopia dist/kopia_darwin_amd64/kopia
ifneq ($(MACOS_SIGNING_IDENTITY),)
	codesign -v --keychain $(MACOS_KEYCHAIN) -s $(MACOS_SIGNING_IDENTITY) --force dist/kopia_darwin_amd64/kopia
	codesign -v --keychain $(MACOS_KEYCHAIN) -s $(MACOS_SIGNING_IDENTITY) --force dist/kopia_darwin_arm64/kopia
	codesign -v --keychain $(MACOS_KEYCHAIN) -s $(MACOS_SIGNING_IDENTITY) --force dist/kopia_darwin_universal/kopia
endif
	tools/make-tgz.sh dist kopia-$(KOPIA_VERSION_NO_PREFIX)-macOS-x64 dist/kopia_darwin_amd64/kopia
	tools/make-tgz.sh dist kopia-$(KOPIA_VERSION_NO_PREFIX)-macOS-arm64 dist/kopia_darwin_arm64/kopia
	tools/make-tgz.sh dist kopia-$(KOPIA_VERSION_NO_PREFIX)-macOS-universal dist/kopia_darwin_universal/kopia

# on Windows build and sign AMD64 and *.zip file
dist/kopia_windows_amd64/kopia.exe: htmlui/build/index.html $(all_go_sources)
	$(MAKE) signing-tools
	GOOS=windows GOARCH=amd64 go build $(KOPIA_BUILD_FLAGS) -o dist/kopia_windows_amd64/kopia.exe -tags embedhtml
ifneq ($(WINDOWS_SIGN_TOOL),)
	$(WINDOWS_SIGN_TOOL) sign /sha1 $(WINDOWS_CERT_SHA1) /fd sha256 /tr "http://time.certum.pl" /f dist\kopia_windows_amd64\kopia.exe
endif
	mkdir -p dist/kopia-$(KOPIA_VERSION_NO_PREFIX)-windows-x64
	cp dist/kopia_windows_amd64/kopia.exe LICENSE README.md dist/kopia-$(KOPIA_VERSION_NO_PREFIX)-windows-x64
	(cd dist && zip -r kopia-$(KOPIA_VERSION_NO_PREFIX)-windows-x64.zip kopia-$(KOPIA_VERSION_NO_PREFIX)-windows-x64)
	rm -rf dist/kopia-$(KOPIA_VERSION_NO_PREFIX)-windows-x64

# On Linux use use goreleaser which will build Kopia for all supported Linux architectures
# and creates .tar.gz, rpm and deb packages.
dist/kopia_linux_amd64/kopia dist/kopia_linux_arm64/kopia dist/kopia_linux_arm_6/kopia: htmlui/build/index.html $(all_go_sources)
ifeq ($(GOARCH),amd64)
	make goreleaser
else
	go build $(KOPIA_BUILD_FLAGS) -o $(kopia_ui_embedded_exe) -tags embedhtml
endif

# builds kopia CLI binary that will be later used as a server for kopia-ui.
kopia: $(kopia_ui_embedded_exe)

kopia-ui-pr-test: app-node-modules htmlui-node-modules
	$(MAKE) html-ui-tests kopia-ui

ci-build:
	$(retry) $(MAKE) kopia
ifeq ($(GOARCH),amd64)
	$(retry) $(MAKE) kopia-ui
endif

ci-tests: lint vet test-with-coverage

ci-integration-tests: integration-tests robustness-tool-tests
	$(MAKE) stress-test

ci-publish:
ifeq ($(GOOS)/$(GOARCH),linux/amd64)
	$(MAKE) create-long-term-repository
	$(MAKE) publish-coverage-results
endif

publish-coverage-results:
	-bash -c "bash <(curl -s https://codecov.io/bash) -f coverage.txt"

# goreleaser - builds packages for all platforms when on linux/amd64,
# but don't publish here, we'll upload to GitHub separately.
GORELEASER_OPTIONS=--rm-dist --parallelism=6 --skip-publish

ifneq ($(PUBLISH_BINARIES)/$(IS_PULL_REQUEST)/$(GOOS)/$(GOARCH),true/false/linux/amd64)
	GORELEASER_OPTIONS+=--skip-sign
endif

ifeq ($(CI_TAG),)
	GORELEASER_OPTIONS+=--snapshot
endif

print_build_info:
	@echo CI_TAG: $(CI_TAG)
	@echo IS_PULL_REQUEST: $(IS_PULL_REQUEST)
	@echo GOOS: $(GOOS)
	@echo GOARCH: $(GOARCH)

goreleaser: export GITHUB_REPOSITORY:=$(GITHUB_REPOSITORY)
goreleaser: $(goreleaser) print_build_info
	-git diff | cat
	$(goreleaser) release $(GORELEASER_OPTIONS)

dev-deps:
	GO111MODULE=off go get -u golang.org/x/tools/cmd/gorename
	GO111MODULE=off go get -u golang.org/x/tools/cmd/guru
	GO111MODULE=off go get -u github.com/nsf/gocode
	GO111MODULE=off go get -u github.com/rogpeppe/godef
	GO111MODULE=off go get -u github.com/lukehoban/go-outline
	GO111MODULE=off go get -u github.com/newhook/go-symbols
	GO111MODULE=off go get -u github.com/sqs/goreturns

test-with-coverage: export RCLONE_EXE=$(rclone)
test-with-coverage: $(gotestsum) $(rclone)
	$(GO_TEST) -count=$(REPEAT_TEST) -covermode=atomic -coverprofile=coverage.txt --coverpkg $(COVERAGE_PACKAGES) -timeout 300s ./...

test: export RCLONE_EXE=$(rclone)
test: GOTESTSUM_FLAGS=--format=$(GOTESTSUM_FORMAT) --no-summary=skipped --jsonfile=.tmp.unit-tests.json
test: $(gotestsum) $(rclone)
	$(GO_TEST) -count=$(REPEAT_TEST) -timeout $(UNIT_TESTS_TIMEOUT) ./...
	-$(gotestsum) tool slowest --jsonfile .tmp.unit-tests.json  --threshold 1000ms

vtest: $(gotestsum)
	$(GO_TEST) -count=$(REPEAT_TEST) -short -v -timeout $(UNIT_TESTS_TIMEOUT) ./...

build-integration-test-binary:
	go build $(KOPIA_BUILD_FLAGS) -o $(KOPIA_INTEGRATION_EXE) -tags testing github.com/kopia/kopia

$(TESTING_ACTION_EXE): tests/testingaction/main.go
	go build -o $(TESTING_ACTION_EXE) -tags testing github.com/kopia/kopia/tests/testingaction

integration-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
integration-tests: export TESTING_ACTION_EXE ?= $(TESTING_ACTION_EXE)
integration-tests: GOTESTSUM_FLAGS=--format=testname --no-summary=skipped --jsonfile=.tmp.integration-tests.json
integration-tests: build-integration-test-binary $(gotestsum) $(TESTING_ACTION_EXE)
	 $(GO_TEST) $(TEST_FLAGS) -count=$(REPEAT_TEST) -parallel $(PARALLEL) -timeout 3600s github.com/kopia/kopia/tests/end_to_end_test
	 -$(gotestsum) tool slowest --jsonfile .tmp.integration-tests.json  --threshold 1000ms

endurance-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
endurance-tests: build-integration-test-binary $(gotestsum)
	 $(GO_TEST) $(TEST_FLAGS) -count=$(REPEAT_TEST) -parallel $(PARALLEL) -timeout 3600s github.com/kopia/kopia/tests/endurance_test

robustness-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
robustness-tests: GOTESTSUM_FORMAT=testname
robustness-tests: build-integration-test-binary $(gotestsum)
	FIO_DOCKER_IMAGE=$(FIO_DOCKER_TAG) \
	$(GO_TEST) -count=$(REPEAT_TEST) github.com/kopia/kopia/tests/robustness/robustness_test $(TEST_FLAGS)

robustness-tool-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
robustness-tool-tests: export FIO_DOCKER_IMAGE=$(FIO_DOCKER_TAG)
robustness-tool-tests: build-integration-test-binary $(gotestsum)
ifeq ($(GOOS)/$(GOARCH),linux/amd64)
	$(GO_TEST) -count=$(REPEAT_TEST) github.com/kopia/kopia/tests/tools/... github.com/kopia/kopia/tests/robustness/engine/... $(TEST_FLAGS)
endif

stress_test: export KOPIA_LONG_STRESS_TEST=1
stress-test: $(gotestsum)
	$(GO_TEST) -count=$(REPEAT_TEST) -timeout 200s github.com/kopia/kopia/tests/stress_test
	$(GO_TEST) -count=$(REPEAT_TEST) -timeout 200s github.com/kopia/kopia/tests/repository_stress_test

layering-test:
ifneq ($(GOOS),windows)
	# verify that code under repo/ can only import code also under repo/ + some
	# whitelisted internal packages.
	find repo/ -name '*.go' | xargs grep "^\t\"github.com/kopia/kopia" \
	   | grep -v -e github.com/kopia/kopia/repo \
	             -e github.com/kopia/kopia/internal \
	             -e github.com/kopia/kopia/issues && exit 1 || echo repo/ layering ok
endif

godoc:
	godoc -http=:33333

coverage: test-with-coverage coverage-html

coverage-html:
	go tool cover -html=coverage.txt

official-release:
	git tag $(RELEASE_VERSION) -m $(RELEASE_VERSION)
	git push -u upstream $(RELEASE_VERSION)

goreturns:
	find . -name '*.go' | xargs goreturns -w --local github.com/kopia/kopia

# see if we have access to credentials encryption key
ifeq ($(CREDENTIAL_ENCRYPTION_KEY),)

ci-credentials:
	@echo CI credentials not available.

ci-gpg-key:
	@echo Not installing GPG keys.

else

ci-gpg-key:
ifneq ($(GOOS),windows)
	openssl aes-256-cbc -K "$(CREDENTIAL_ENCRYPTION_KEY)" -iv "$(CREDENTIAL_ENCRYPTION_IV)" -in kopia.gpg.enc -out /tmp/kopia.gpg -d
	gpg --import /tmp/kopia.gpg
endif

ci-credentials: ci-gpg-key

ifneq ($(GOOS),windows)
	@echo Installing GPG key...
	openssl aes-256-cbc -K "$(CREDENTIAL_ENCRYPTION_KEY)" -iv "$(CREDENTIAL_ENCRYPTION_IV)" -in kopia.gpg.enc -out /tmp/kopia.gpg -d
	gpg --import /tmp/kopia.gpg
	openssl aes-256-cbc -K "$(CREDENTIAL_ENCRYPTION_KEY)" -iv "$(CREDENTIAL_ENCRYPTION_IV)" -in tests/credentials/gcs/test_service_account.json.enc -out repo/blob/gcs/test_service_account.json -d
	openssl aes-256-cbc -K "$(CREDENTIAL_ENCRYPTION_KEY)" -iv "$(CREDENTIAL_ENCRYPTION_IV)" -in tests/credentials/sftp/id_kopia.enc -out repo/blob/sftp/id_kopia -d
	openssl aes-256-cbc -K "$(CREDENTIAL_ENCRYPTION_KEY)" -iv "$(CREDENTIAL_ENCRYPTION_IV)" -in tests/credentials/sftp/known_hosts.enc -out repo/blob/sftp/known_hosts -d
	openssl aes-256-cbc -K "$(CREDENTIAL_ENCRYPTION_KEY)" -iv "$(CREDENTIAL_ENCRYPTION_IV)" -in tools/boto.enc -out tools/.boto -d

ifeq ($(GOARCH),amd64)
	$(MAKE) install-google-cloud-sdk-if-not-present
	$(HOME)/google-cloud-sdk/bin/gcloud auth activate-service-account --key-file repo/blob/gcs/test_service_account.json
endif
endif

endif

install-google-cloud-sdk-if-not-present:
	if [ ! -d $(HOME)/google-cloud-sdk ]; then $(retry) $(MAKE) install-google-cloud-sdk; fi

install-google-cloud-sdk:
	-rm -rf $(HOME)/google-cloud-sdk
	echo Installing Google Cloud SDK...
	curl -s https://sdk.cloud.google.com | CLOUDSDK_CORE_DISABLE_PROMPTS=1 bash 2>/dev/null >/dev/null
	echo Finished Installing Google Cloud SDK.

RELEASE_STAGING_DIR=$(CURDIR)/.release

stage-release:
	rm -rf $(RELEASE_STAGING_DIR)
	mkdir -p $(RELEASE_STAGING_DIR)
	find dist -type f -exec cp -v {} $(RELEASE_STAGING_DIR) \;
	(cd $(RELEASE_STAGING_DIR) && sha256sum * > checksums.txt)
	cat $(RELEASE_STAGING_DIR)/checksums.txt
ifneq ($(CREDENTIAL_ENCRYPTION_KEY),)
	gpg --output $(RELEASE_STAGING_DIR)/checksums.txt.sig --detach-sig $(RELEASE_STAGING_DIR)/checksums.txt
endif

ifeq ($(IS_PULL_REQUEST),false)
ifneq ($(CI_TAG),)
GH_RELEASE_REPO=$(GITHUB_REPOSITORY)
GH_RELEASE_FLAGS=--draft
GH_RELEASE_NAME=v$(KOPIA_VERSION_NO_PREFIX)
else
ifeq ($(GITHUB_REF),refs/heads/master)
ifneq ($(NON_TAG_RELEASE_REPO),)
GH_RELEASE_REPO=$(REPO_OWNER)/$(NON_TAG_RELEASE_REPO)
GH_RELEASE_FLAGS=
GH_RELEASE_NAME=v$(KOPIA_VERSION_NO_PREFIX)
endif
endif
endif
endif

push-github-release: $(github_release)
ifneq ($(GH_RELEASE_REPO),)
	@echo Creating Github Release $(GH_RELEASE_NAME) in $(GH_RELEASE_REPO) with flags $(GH_RELEASE_FLAGS)
	gh --repo $(GH_RELEASE_REPO) release view $(GH_RELEASE_NAME) || gh --repo $(GH_RELEASE_REPO) release create $(GH_RELEASE_FLAGS) $(GH_RELEASE_NAME)
	gh --repo $(GH_RELEASE_REPO) release upload $(GH_RELEASE_NAME) $(RELEASE_STAGING_DIR)/*
else
	@echo Not creating Github Release
endif

ifneq ($(CI_TAG),)

create-long-term-repository: build-integration-test-binary

ifeq ($(REPO_OWNER),kopia)
	echo Creating long-term repository $(CI_TAG)...
	KOPIA_EXE=$(KOPIA_INTEGRATION_EXE) ./tests/compat_test/gen-compat-repo.sh
else
	@echo Not creating long-term repository from a fork.
endif

else

create-long-term-repository:
	echo Not creating long-term repository.

endif

publish-packages:
ifeq ($(REPO_OWNER)/$(GOOS)/$(GOARCH)/$(IS_PULL_REQUEST),kopia/linux/amd64/false)
	$(CURDIR)/tools/apt-publish.sh $(CURDIR)/dist
	$(CURDIR)/tools/rpm-publish.sh $(CURDIR)/dist
	$(CURDIR)/tools/homebrew-publish.sh $(CURDIR)/dist $(KOPIA_VERSION_NO_PREFIX)
	$(CURDIR)/tools/scoop-publish.sh $(CURDIR)/dist $(KOPIA_VERSION_NO_PREFIX)
	@echo $(DOCKERHUB_TOKEN) | docker login --username $(DOCKERHUB_USERNAME) --password-stdin
	$(CURDIR)/tools/docker-publish.sh $(CURDIR)/dist_binaries
else
	@echo Not pushing packages on pull request builds.
endif

PERF_BENCHMARK_INSTANCE=kopia-perf
PERF_BENCHMARK_INSTANCE_ZONE=us-west1-a
PERF_BENCHMARK_CHANNEL=testing
PERF_BENCHMARK_VERSION=0.6.4
PERF_BENCHMARK_TOTAL_SIZE=20G

perf-benchmark-setup:
	gcloud compute instances create $(PERF_BENCHMARK_INSTANCE) --machine-type n1-standard-8 --zone=$(PERF_BENCHMARK_INSTANCE_ZONE) --local-ssd interface=nvme
	# wait for instance to boot
	sleep 20
	gcloud compute scp tests/perf_benchmark/perf-benchmark-setup.sh $(PERF_BENCHMARK_INSTANCE):. --zone=$(PERF_BENCHMARK_INSTANCE_ZONE)
	gcloud compute ssh $(PERF_BENCHMARK_INSTANCE) --zone=$(PERF_BENCHMARK_INSTANCE_ZONE) --command "./perf-benchmark-setup.sh"

perf-benchmark-teardown:
	gcloud compute instances delete $(PERF_BENCHMARK_INSTANCE) --zone=$(PERF_BENCHMARK_INSTANCE_ZONE)

perf-benchmark-test:
	gcloud compute scp tests/perf_benchmark/perf-benchmark.sh $(PERF_BENCHMARK_INSTANCE):. --zone=$(PERF_BENCHMARK_INSTANCE_ZONE)
	gcloud compute ssh $(PERF_BENCHMARK_INSTANCE) --zone=$(PERF_BENCHMARK_INSTANCE_ZONE) --command "./perf-benchmark.sh $(PERF_BENCHMARK_VERSION) $(PERF_BENCHMARK_CHANNEL) $(PERF_BENCHMARK_TOTAL_SIZE)"

perf-benchmark-test-all:
	$(MAKE) perf-benchmark-test PERF_BENCHMARK_VERSION=0.4.0
	$(MAKE) perf-benchmark-test PERF_BENCHMARK_VERSION=0.5.2
	$(MAKE) perf-benchmark-test PERF_BENCHMARK_VERSION=0.6.4
	$(MAKE) perf-benchmark-test PERF_BENCHMARK_VERSION=0.7.0~rc1

perf-benchmark-results:
	gcloud compute scp $(PERF_BENCHMARK_INSTANCE):psrecord-* tests/perf_benchmark --zone=$(PERF_BENCHMARK_INSTANCE_ZONE) 
	gcloud compute scp $(PERF_BENCHMARK_INSTANCE):repo-size-* tests/perf_benchmark --zone=$(PERF_BENCHMARK_INSTANCE_ZONE)
	(cd tests/perf_benchmark && go run process_results.go)
