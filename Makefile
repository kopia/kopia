COVERAGE_PACKAGES=github.com/kopia/kopia/repo/...,github.com/kopia/kopia/fs/...,github.com/kopia/kopia/snapshot/...
TEST_FLAGS?=
KOPIA_INTEGRATION_EXE=$(CURDIR)/dist/integration/kopia.exe
TESTING_ACTION_EXE=$(CURDIR)/dist/integration/testingaction.exe
FIO_DOCKER_TAG=ljishen/fio

export BOTO_PATH=$(CURDIR)/tools/.boto

all: test lint vet integration-tests

retry=

ifneq ($(TRAVIS_OS_NAME),)
retry=$(CURDIR)/tools/retry.sh
endif

include tools/tools.mk

GOTESTSUM_FORMAT=pkgname-and-test-fails
GO_TEST=$(gotestsum) --format=$(GOTESTSUM_FORMAT) --no-summary=skipped --

LINTER_DEADLINE=300s
UNIT_TESTS_TIMEOUT=300s

ifeq ($(kopia_arch_name),amd64)
PARALLEL=8
else
# tweaks for less powerful platforms
PARALLEL=2
endif

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
	$(linter) --deadline $(LINTER_DEADLINE) run $(linter_flags)

lint-and-log: $(linter)
	$(linter) --deadline $(LINTER_DEADLINE) run $(linter_flags) | tee .linterr.txt


vet-time-inject:
ifneq ($(TRAVIS_OS_NAME),windows)
	! find . -name '*.go' \
	-exec grep -n -e time.Now -e time.Since -e time.Until {} + \
	grep -v src/golang.org | grep -v -e allow:no-inject-time
endif

vet: vet-time-inject
	go vet -all .

go-modules:
	go mod download

app-node-modules: $(npm)
ifeq ($(kopia_arch_name),amd64)
	make -C app node_modules
endif

htmlui-node-modules: $(npm)
	make -C htmlui node_modules

travis-setup: travis-install-gpg-key travis-install-test-credentials go-modules all-tools
ifneq ($(TRAVIS_OS_NAME),)
	-git checkout go.mod go.sum
endif

website:
	$(MAKE) -C site build

html-ui: htmlui-node-modules
	$(MAKE) -C htmlui build-html CI=true

html-ui-tests: htmlui-node-modules
	$(MAKE) -C htmlui test CI=true

html-ui-bindata: html-ui $(go_bindata)
	(cd htmlui/build && $(go_bindata) -fs -tags embedhtml -o "$(CURDIR)/internal/server/htmlui_bindata.go" -pkg server -ignore '.map' . static/css static/js static/media)

html-ui-bindata-fallback: $(go_bindata)
	(cd internal/server && $(go_bindata) -fs -tags !embedhtml -o "$(CURDIR)/internal/server/htmlui_fallback.go" -pkg server index.html)

kopia-ui:
	$(MAKE) -C app build-electron

# build-current-os-noui compiles a binary for the current os/arch in the same location as goreleaser
# kopia-ui build needs this particular location to embed the correct server binary.
# note we're not building or embedding HTML UI to speed up PR testing process.
build-current-os-noui:
	go build -o dist/kopia_$(shell go env GOOS)_$(shell go env GOARCH)$(exe_suffix)

build-current-os-with-ui: html-ui-bindata
	go build -o dist/kopia_$(shell go env GOOS)_$(shell go env GOARCH)$(exe_suffix) -tags embedhtml

kopia-ui-pr-test: app-node-modules htmlui-node-modules
	$(MAKE) build-current-os-with-ui
	$(MAKE) html-ui-tests kopia-ui

ifeq ($(kopia_arch_name),arm64)
travis-release:
	$(MAKE) test
	$(MAKE) integration-tests
	$(MAKE) lint
else

travis-release:
ifeq ($(TRAVIS_PULL_REQUEST),false)
	$(retry) $(MAKE) goreleaser
	$(retry) $(MAKE) kopia-ui
else
	$(retry) $(MAKE) build-current-os-noui
endif
	$(MAKE) lint vet test-with-coverage
	$(retry) $(MAKE) layering-test
	$(retry) $(MAKE) integration-tests
ifeq ($(TRAVIS_OS_NAME),linux)
	$(MAKE) publish-packages
	$(MAKE) robustness-tool-tests
	$(MAKE) website
	$(MAKE) stress-test
	$(MAKE) travis-create-long-term-repository
	$(MAKE) upload-coverage
endif

endif

# goreleaser - builds binaries for all platforms
GORELEASER_OPTIONS=--rm-dist --parallelism=6

sign_gpg=1
publish_binaries=1

ifneq ($(TRAVIS_PULL_REQUEST),false)
	# not running on travis, or travis in PR mode, skip signing
	sign_gpg=0
endif

# publish and sign only from linux/amd64 to avoid duplicates
ifneq ($(TRAVIS_OS_NAME)/$(kopia_arch_name),linux/amd64)
	sign_gpg=0
	publish_binaries=0
endif

ifeq ($(sign_gpg),0)
GORELEASER_OPTIONS+=--skip-sign
endif

# publish only from tagged releases
ifeq ($(TRAVIS_TAG),)
	GORELEASER_OPTIONS+=--snapshot
	publish_binaries=0
endif

ifeq ($(publish_binaries),0)
GORELEASER_OPTIONS+=--skip-publish
endif

print_build_info:
	@echo TRAVIS_TAG: $(TRAVIS_TAG)
	@echo TRAVIS_PULL_REQUEST: $(TRAVIS_PULL_REQUEST)
	@echo TRAVIS_OS_NAME: $(TRAVIS_OS_NAME)

goreleaser: $(goreleaser) print_build_info
	-git diff | cat
	$(goreleaser) release $(GORELEASER_OPTIONS)

ifeq ($(TRAVIS_PULL_REQUEST),false)

upload-coverage: $(GOVERALLS_TOOL)
	$(GOVERALLS_TOOL) -service=travis-ci -coverprofile=tmp.cov

else

upload-coverage:
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
	$(GO_TEST) -count=1 -coverprofile=tmp.cov --coverpkg $(COVERAGE_PACKAGES) -timeout 300s $(shell go list ./...)

test-with-coverage-pkgonly:
	$(GO_TEST) -count=1 -coverprofile=tmp.cov -timeout 300s github.com/kopia/kopia/...

test: $(gotestsum)
	$(GO_TEST) -count=1 -timeout $(UNIT_TESTS_TIMEOUT) ./...

vtest: $(gotestsum)
	$(GO_TEST) -count=1 -short -v -timeout $(UNIT_TESTS_TIMEOUT) ./...

build-integration-test-binary:
	go build -o $(KOPIA_INTEGRATION_EXE) -tags testing github.com/kopia/kopia

$(TESTING_ACTION_EXE): tests/testingaction/main.go
	go build -o $(TESTING_ACTION_EXE) -tags testing github.com/kopia/kopia/tests/testingaction

integration-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
integration-tests: export TESTING_ACTION_EXE ?= $(TESTING_ACTION_EXE)
integration-tests: build-integration-test-binary $(gotestsum) $(TESTING_ACTION_EXE)
	 $(GO_TEST) $(TEST_FLAGS) -count=1 -parallel $(PARALLEL) -timeout 3600s github.com/kopia/kopia/tests/end_to_end_test

endurance-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
endurance-tests: build-integration-test-binary $(gotestsum)
	 $(GO_TEST) $(TEST_FLAGS) -count=1 -parallel $(PARALLEL) -timeout 3600s github.com/kopia/kopia/tests/endurance_test

robustness-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
robustness-tests: build-integration-test-binary $(gotestsum)
	FIO_DOCKER_IMAGE=$(FIO_DOCKER_TAG) \
	$(GO_TEST) -count=1 github.com/kopia/kopia/tests/robustness/robustness_test $(TEST_FLAGS)

robustness-tool-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
robustness-tool-tests: build-integration-test-binary $(gotestsum)
	KOPIA_EXE=$(KOPIA_INTEGRATION_EXE) \
	FIO_DOCKER_IMAGE=$(FIO_DOCKER_TAG) \
	$(GO_TEST) -count=1 github.com/kopia/kopia/tests/tools/... github.com/kopia/kopia/tests/robustness/engine/... $(TEST_FLAGS)

stress-test: $(gotestsum)
	KOPIA_LONG_STRESS_TEST=1 $(GO_TEST) -count=1 -timeout 200s github.com/kopia/kopia/tests/stress_test
	$(GO_TEST) -count=1 -timeout 200s github.com/kopia/kopia/tests/repository_stress_test

layering-test:
ifneq ($(uname),Windows)
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
	go tool cover -html=tmp.cov

official-release:
	git tag $(RELEASE_VERSION) -m $(RELEASE_VERSION)
	git push -u upstream $(RELEASE_VERSION)

goreturns:
	find . -name '*.go' | xargs goreturns -w --local github.com/kopia/kopia

# this indicates we're running on Travis CI and NOT processing pull request.
ifeq ($(TRAVIS_PULL_REQUEST),false)

# https://travis-ci.community/t/windows-build-timeout-after-success-ps-shows-gpg-agent/4967/4

travis-install-gpg-key:
ifeq ($(TRAVIS_OS_NAME),windows)
	@echo Not installing GPG key on Windows...
else
	@echo Installing GPG key...
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in kopia.gpg.enc -out /tmp/kopia.gpg -d
	gpg --import /tmp/kopia.gpg
endif

travis-install-test-credentials:
	@echo Installing test credentials...
ifneq ($(TRAVIS_OS_NAME),windows)
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in tests/credentials/gcs/test_service_account.json.enc -out repo/blob/gcs/test_service_account.json -d
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in tests/credentials/sftp/id_kopia.enc -out repo/blob/sftp/id_kopia -d
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in tests/credentials/sftp/known_hosts.enc -out repo/blob/sftp/known_hosts -d
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in tools/boto.enc -out tools/.boto -d
endif

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

travis-create-long-term-repository: build-integration-test-binary travis-install-cloud-sdk
	echo Creating long-term repository $(TRAVIS_TAG)...
	KOPIA_EXE=$(KOPIA_INTEGRATION_EXE) ./tests/compat_test/gen-compat-repo.sh

else

travis-create-long-term-repository:
	echo Not creating long-term repository.

endif

ifeq ($(TRAVIS_OS_NAME)/$(kopia_arch_name)/$(TRAVIS_PULL_REQUEST),linux/amd64/false)
publish-packages:
	$(CURDIR)/tools/apt-publish.sh $(CURDIR)/dist
	$(CURDIR)/tools/rpm-publish.sh $(CURDIR)/dist
else
publish-packages:
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


