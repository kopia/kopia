COVERAGE_PACKAGES=github.com/kopia/kopia/repo/...,github.com/kopia/kopia/fs/...,github.com/kopia/kopia/snapshot/...
TEST_FLAGS?=
KOPIA_INTEGRATION_EXE=$(CURDIR)/dist/integration/kopia.exe
TESTING_ACTION_EXE=$(CURDIR)/dist/integration/testingaction.exe
FIO_DOCKER_TAG=ljishen/fio
REPEAT_TEST=1

export BOTO_PATH=$(CURDIR)/tools/.boto

all: test lint vet integration-tests

retry:=$(CURDIR)/tools/retry.sh

include tools/tools.mk

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

quick-install:
	# same as install but assumes HTMLUI has been built
	go install $(KOPIA_BUILD_FLAGS) -tags embedhtml

install-noui:
	go install $(KOPIA_BUILD_FLAGS)

escape-analysis:
	go build -gcflags '-m -l' github.com/kopia/kopia/...

clean:
	make clean-tools
	make -C htmlui clean

play:
	go run cmd/playground/main.go

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
	make -C app node_modules
endif

htmlui-node-modules: $(npm)
	make -C htmlui node_modules

ci-setup: ci-credentials go-modules all-tools
ifeq ($(CI),true)
	-git checkout go.mod go.sum
endif

website:
	$(MAKE) -C site build

html-ui: htmlui-node-modules
	$(MAKE) -C htmlui build-html CI=true

html-ui-tests: htmlui-node-modules
	$(MAKE) -C htmlui test CI=true

kopia-ui:
	$(MAKE) -C app build-electron

# build-current-os-noui compiles a binary for the current os/arch in the same location as goreleaser
# kopia-ui build needs this particular location to embed the correct server binary.
# note we're not building or embedding HTML UI to speed up PR testing process.
build-current-os-noui:
	go build $(KOPIA_BUILD_FLAGS) -o dist/kopia_$(GOOS)_$(GOARCH)/kopia$(exe_suffix)

build-current-os-with-ui: html-ui
ifeq ($(GOOS)/$(CI),darwin/true)
	# build a fat binary that runs on both AMD64 and ARM64, this will be embedded in KopiaUI
	# and will run optimally on both architectures.
	GOARCH=arm64 go build $(KOPIA_BUILD_FLAGS) -o dist/kopia_darwin_arm64/.kopia-arm64 -tags embedhtml
	GOARCH=amd64 go build $(KOPIA_BUILD_FLAGS) -o dist/kopia_darwin_amd64/.kopia-amd64 -tags embedhtml
	lipo -create -output dist/kopia_darwin_$(GOARCH)/kopia dist/kopia_darwin_arm64/.kopia-arm64 dist/kopia_darwin_amd64/.kopia-amd64
ifneq ($(MACOS_SIGNING_IDENTITY),)
	codesign -v --keychain $(MACOS_KEYCHAIN) -s $(MACOS_SIGNING_IDENTITY) --force dist/kopia_$(GOOS)_$(GOARCH)/kopia$(exe_suffix)
endif
else
	go build $(KOPIA_BUILD_FLAGS) -o dist/kopia_$(GOOS)_$(GOARCH)/kopia$(exe_suffix) -tags embedhtml
endif

kopia-ui-pr-test: app-node-modules htmlui-node-modules
	$(MAKE) build-current-os-with-ui
	$(MAKE) html-ui-tests kopia-ui

ci-build:
ifeq ($(IS_PULL_REQUEST),true)
	# PR mode - run very quick build of just the binary without embedded UI.
	$(retry) $(MAKE) build-current-os-noui
else

ifeq ($(GOOS)/$(GOARCH),linux/amd64)
	# on linux/amd64 run goreleaser which publishes Kopia binaries, and packages for all platforms
	$(retry) $(MAKE) goreleaser
else
	# everywhere else (windows, mac, arm linux) build only kopia binary in the same location so we
	# can later build the UI.
	$(retry) $(MAKE) build-current-os-with-ui
endif

ifeq ($(GOARCH),amd64)
	$(retry) $(MAKE) kopia-ui
endif

endif

ci-tests: lint vet test-with-coverage

ci-integration-tests: integration-tests robustness-tool-tests
	$(MAKE) stress-test

ci-publish:
ifeq ($(GOOS)/$(GOARCH),linux/amd64)
	$(MAKE) publish-packages
	$(MAKE) create-long-term-repository
	$(MAKE) publish-coverage-results
endif

publish-coverage-results:
	-bash -c "bash <(curl -s https://codecov.io/bash) -f coverage.txt"

# goreleaser - builds binaries for all platforms
GORELEASER_OPTIONS=--rm-dist --parallelism=6

sign_gpg=1
publish_binaries=1

ifneq ($(PUBLISH_BINARIES),true)
	publish_binaries=0
	sign_gpg=0
endif

ifneq ($(IS_PULL_REQUEST),false)
	# not running on CI, or CI in PR mode, skip signing
	sign_gpg=0
endif

# publish and sign only from linux/amd64 to avoid duplicates
ifneq ($(GOOS)/$(GOARCH),linux/amd64)
	sign_gpg=0
	publish_binaries=0
endif

ifeq ($(sign_gpg),0)
GORELEASER_OPTIONS+=--skip-sign
endif

# publish only from tagged releases
ifeq ($(CI_TAG),)
	GORELEASER_OPTIONS+=--snapshot
	publish_binaries=0
endif

ifeq ($(publish_binaries),0)
GORELEASER_OPTIONS+=--skip-publish
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

else

ci-credentials:

ifneq ($(GOOS),windows)
	@echo Installing GPG key...
	openssl aes-256-cbc -K "$(CREDENTIAL_ENCRYPTION_KEY)" -iv "$(CREDENTIAL_ENCRYPTION_IV)" -in kopia.gpg.enc -out /tmp/kopia.gpg -d
	gpg --import /tmp/kopia.gpg
	openssl aes-256-cbc -K "$(CREDENTIAL_ENCRYPTION_KEY)" -iv "$(CREDENTIAL_ENCRYPTION_IV)" -in tests/credentials/gcs/test_service_account.json.enc -out repo/blob/gcs/test_service_account.json -d
	openssl aes-256-cbc -K "$(CREDENTIAL_ENCRYPTION_KEY)" -iv "$(CREDENTIAL_ENCRYPTION_IV)" -in tests/credentials/sftp/id_kopia.enc -out repo/blob/sftp/id_kopia -d
	openssl aes-256-cbc -K "$(CREDENTIAL_ENCRYPTION_KEY)" -iv "$(CREDENTIAL_ENCRYPTION_IV)" -in tests/credentials/sftp/known_hosts.enc -out repo/blob/sftp/known_hosts -d
	openssl aes-256-cbc -K "$(CREDENTIAL_ENCRYPTION_KEY)" -iv "$(CREDENTIAL_ENCRYPTION_IV)" -in tools/boto.enc -out tools/.boto -d

ifeq ($(GOARCH),amd64)
	if [ ! -d $(HOME)/google-cloud-sdk ]; then curl https://sdk.cloud.google.com | CLOUDSDK_CORE_DISABLE_PROMPTS=1 bash; fi
	$(HOME)/google-cloud-sdk/bin/gcloud auth activate-service-account --key-file repo/blob/gcs/test_service_account.json
endif
endif

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
else
	@echo Not pushing to Linux repositories on pull request builds.
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
