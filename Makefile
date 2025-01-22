COVERAGE_PACKAGES=./repo/...,./fs/...,./snapshot/...,./cli/...,./internal/...,./notification/...
TEST_FLAGS?=
KOPIA_INTEGRATION_EXE=$(CURDIR)/dist/testing_$(GOOS)_$(GOARCH)/kopia.exe
TESTING_ACTION_EXE=$(CURDIR)/dist/testing_$(GOOS)_$(GOARCH)/testingaction.exe
FIO_DOCKER_TAG=ljishen/fio
REPEAT_TEST=1

# get a list all go files
rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) $(filter $(subst *,%,$2),$d))
go_source_dirs=cli fs internal repo snapshot
all_go_sources=$(foreach d,$(go_source_dirs),$(call rwildcard,$d/,*.go)) $(wildcard *.go)

all:
	$(MAKE) test
	$(MAKE) lint

include tools/tools.mk

kopia_ui_embedded_exe=dist/kopia_$(GOOS)_$(GOARCH)/kopia$(exe_suffix)

ifeq ($(GOOS),darwin)
	# on macOS, Kopia uses universal binary that works for AMD64 and ARM64
	kopia_ui_embedded_exe=dist/kopia_darwin_universal/kopia
endif

ifeq ($(GOOS),linux)

ifeq ($(GOARCH),arm)
	kopia_ui_embedded_exe=dist/kopia_linux_armv7l/kopia
endif

ifeq ($(GOARCH),amd64)
	kopia_ui_embedded_exe=dist/kopia_linux_x64/kopia
endif

endif

GOTESTSUM_FORMAT=pkgname-and-test-fails
GOTESTSUM_FLAGS=--format=$(GOTESTSUM_FORMAT) --no-summary=skipped
GO_TEST?=$(gotestsum) $(GOTESTSUM_FLAGS) --

LINTER_DEADLINE=1200s
UNIT_TESTS_TIMEOUT=1200s

ifeq ($(GOARCH),amd64)
PARALLEL=8
else
# tweaks for less powerful platforms
PARALLEL=2
endif

-include ./Makefile.local.mk

install:
	go install $(KOPIA_BUILD_FLAGS) -tags "$(KOPIA_BUILD_TAGS)" github.com/kopia/kopia

install-noui: KOPIA_BUILD_TAGS=nohtmlui
install-noui: install

install-race:
	go install -race $(KOPIA_BUILD_FLAGS) -tags "$(KOPIA_BUILD_TAGS)"

check-locks: $(checklocks)
ifneq ($(GOOS)/$(GOARCH),linux/arm64)
ifneq ($(GOOS)/$(GOARCH),linux/arm)
	go vet -vettool=$(checklocks) ./...
endif
endif

lint: $(linter)
ifneq ($(GOOS)/$(GOARCH),linux/arm64)
ifneq ($(GOOS)/$(GOARCH),linux/arm)
	$(linter) --timeout $(LINTER_DEADLINE) run $(linter_flags)
endif
endif

lint-fix: $(linter)
ifneq ($(GOOS)/$(GOARCH),linux/arm64)
ifneq ($(GOOS)/$(GOARCH),linux/arm)
	$(linter) --timeout $(LINTER_DEADLINE) run --fix $(linter_flags)
endif
endif

lint-and-log: $(linter)
	$(linter) --timeout $(LINTER_DEADLINE) run $(linter_flags) | tee .linterr.txt

lint-all: $(linter)
	GOOS=windows GOARCH=amd64 $(linter) --timeout $(LINTER_DEADLINE) run $(linter_flags)
	GOOS=linux GOARCH=amd64 $(linter) --timeout $(LINTER_DEADLINE) run $(linter_flags)
	GOOS=linux GOARCH=arm64 $(linter) --timeout $(LINTER_DEADLINE) run $(linter_flags)
	GOOS=linux GOARCH=arm $(linter) --timeout $(LINTER_DEADLINE) run $(linter_flags)
	GOOS=darwin GOARCH=amd64 $(linter) --timeout $(LINTER_DEADLINE) run $(linter_flags)
	GOOS=darwin GOARCH=arm64 $(linter) --timeout $(LINTER_DEADLINE) run $(linter_flags)
	GOOS=openbsd GOARCH=amd64 $(linter) --timeout $(LINTER_DEADLINE) run $(linter_flags)
	GOOS=freebsd GOARCH=amd64 $(linter) --timeout $(LINTER_DEADLINE) run $(linter_flags)

vet:
	go vet -all .

go-modules:
	go mod download

app-node-modules: $(npm)
ifeq ($(GOARCH),amd64)
	$(MAKE) -C app deps
endif

ci-setup: go-modules all-tools app-node-modules
ifeq ($(CI),true)
	-git checkout go.mod go.sum
endif

website:
	$(MAKE) -C site build

kopia-ui: $(kopia_ui_embedded_exe)
	$(MAKE) -C app build-electron

MAYBE_XVFB=
ifeq ($(GOOS),linux)
# on Linux
MAYBE_XVFB=xvfb-run --auto-servernum --server-args="-screen 0 1280x960x24" --
endif

kopia-ui-test:
ifeq ($(GOOS)/$(GOARCH),linux/amd64)
	# on Linux we run from installed location due to AppArmor requirement on Ubuntu 24.04
	sudo apt-get install -y ./dist/kopia-ui/kopia-ui*_amd64.deb
endif
	$(MAYBE_XVFB) $(MAKE) -C app e2e-test

# use this to test htmlui changes in full build of KopiaUI, this is rarely needed
# except when testing htmlui specific features that only light up when running under Electron.
#
# You need to have 3 repositories checked out in parallel:
#
#   https://github.com/kopia/kopia
#   https://github.com/kopia/htmlui
#   https://github.com/kopia/htmluibuild

kopia-ui-with-local-htmlui-changes:
	(cd ../htmlui && npm run build && ./push_local.sh)
	rm -f $(kopia_ui_embedded_exe)
	GOWORK=$(CURDIR)/tools/localhtmlui.work $(MAKE) kopia-ui

install-with-local-htmlui-changes: export GOWORK=$(CURDIR)/tools/localhtmlui.work
install-with-local-htmlui-changes:
ifeq ($(GOOS),windows)
	(cd ../htmlui && npm run build && push_local.cmd)
else
	(cd ../htmlui && npm run build && ./push_local.sh)
endif
	$(MAKE) install

# build-current-os-noui compiles a binary for the current os/arch in the same location as goreleaser
# kopia-ui build needs this particular location to embed the correct server binary.
# note we're not building or embedding HTML UI to speed up PR testing process.
build-current-os-noui:
	go build $(KOPIA_BUILD_FLAGS) -o $(kopia_ui_embedded_exe) github.com/kopia/kopia

# on macOS build and sign AMD64, ARM64 and Universal binary and *.tar.gz files for them
dist/kopia_darwin_universal/kopia dist/kopia_darwin_amd64/kopia dist/kopia_darwin_arm6/kopia: $(all_go_sources)
	GOARCH=arm64 go build $(KOPIA_BUILD_FLAGS) -o dist/kopia_darwin_arm64/kopia -tags "$(KOPIA_BUILD_TAGS)" github.com/kopia/kopia
	GOARCH=amd64 go build $(KOPIA_BUILD_FLAGS) -o dist/kopia_darwin_amd64/kopia -tags "$(KOPIA_BUILD_TAGS)" github.com/kopia/kopia
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
dist/kopia_windows_amd64/kopia.exe: $(all_go_sources)
	GOOS=windows GOARCH=amd64 go build $(KOPIA_BUILD_FLAGS) -o dist/kopia_windows_amd64/kopia.exe -tags "$(KOPIA_BUILD_TAGS)" github.com/kopia/kopia
ifneq ($(WINDOWS_SIGN_TOOL),)
	tools/.tools/signtool.exe sign //sha1 $(WINDOWS_CERT_SHA1) //fd sha256 //tr "http://timestamp.digicert.com" //v dist/kopia_windows_amd64/kopia.exe
endif
	mkdir -p dist/kopia-$(KOPIA_VERSION_NO_PREFIX)-windows-x64
	cp dist/kopia_windows_amd64/kopia.exe LICENSE README.md dist/kopia-$(KOPIA_VERSION_NO_PREFIX)-windows-x64
	(cd dist && zip -r kopia-$(KOPIA_VERSION_NO_PREFIX)-windows-x64.zip kopia-$(KOPIA_VERSION_NO_PREFIX)-windows-x64)
	rm -rf dist/kopia-$(KOPIA_VERSION_NO_PREFIX)-windows-x64

# On Linux use use goreleaser which will build Kopia for all supported Linux architectures
# and creates .tar.gz, rpm and deb packages.
dist/kopia_linux_x64/kopia dist/kopia_linux_arm64/kopia dist/kopia_linux_armv7l/kopia: $(all_go_sources)
ifeq ($(GOARCH),amd64)
	$(MAKE) goreleaser
	rm -f dist/kopia_linux_x64
	ln -sf kopia_linux_amd64 dist/kopia_linux_x64
	rm -f dist/kopia_linux_armv7l
	ln -sf kopia_linux_arm_6 dist/kopia_linux_armv7l
else
	go build $(KOPIA_BUILD_FLAGS) -o $(kopia_ui_embedded_exe) -tags "$(KOPIA_BUILD_TAGS)" github.com/kopia/kopia
endif

# builds kopia CLI binary that will be later used as a server for kopia-ui.
kopia: $(kopia_ui_embedded_exe)

ci-build:
# install Apple API key needed to notarize Apple binaries
ifeq ($(GOOS),darwin)
ifneq ($(APPLE_API_KEY_BASE64),)
ifneq ($(APPLE_API_KEY),)
	@ echo "$(APPLE_API_KEY_BASE64)" | base64 -d > "$(APPLE_API_KEY)"
endif
endif
endif
	$(MAKE) kopia
ifneq ($(GOOS)/$(GOARCH),linux/arm64)
	$(retry) $(MAKE) kopia-ui
	$(retry) $(MAKE) kopia-ui-test
endif
ifeq ($(GOOS)/$(GOARCH),linux/amd64)
	$(MAKE) generate-change-log
	$(MAKE) download-rclone
endif

# remove API key
ifeq ($(GOOS),darwin)
ifneq ($(APPLE_API_KEY),)
	@ rm -f "$(APPLE_API_KEY)"
endif
endif


download-rclone:
	go run ./tools/gettool --tool rclone:$(RCLONE_VERSION) --output-dir dist/kopia_linux_amd64/ --goos=linux --goarch=amd64
	go run ./tools/gettool --tool rclone:$(RCLONE_VERSION) --output-dir dist/kopia_linux_arm64/ --goos=linux --goarch=arm64
	go run ./tools/gettool --tool rclone:$(RCLONE_VERSION) --output-dir dist/kopia_linux_arm_6/ --goos=linux --goarch=arm


ci-tests: vet test

ci-integration-tests:
	$(MAKE) robustness-tool-tests socket-activation-tests

ci-publish-coverage:
ifeq ($(GOOS)/$(GOARCH),linux/amd64)
	-bash -c "bash <(curl -s https://codecov.io/bash) -f coverage.txt"
endif

# goreleaser - builds packages for all platforms when on linux/amd64,
# but don't publish here, we'll upload to GitHub separately.
GORELEASER_OPTIONS=--rm-dist --parallelism=6 --skip-publish --skip-sign

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

test-with-coverage: export KOPIA_COVERAGE_TEST=1
test-with-coverage: export GOEXPERIMENT=nocoverageredesign
test-with-coverage: export TESTING_ACTION_EXE ?= $(TESTING_ACTION_EXE)
test-with-coverage: $(gotestsum) $(TESTING_ACTION_EXE)
	$(GO_TEST) $(UNIT_TEST_RACE_FLAGS) -tags testing -count=$(REPEAT_TEST) -short -covermode=atomic -coverprofile=coverage.txt --coverpkg $(COVERAGE_PACKAGES) -timeout $(UNIT_TESTS_TIMEOUT) ./...

test: GOTESTSUM_FLAGS=--format=$(GOTESTSUM_FORMAT) --no-summary=skipped --jsonfile=.tmp.unit-tests.json
test: export TESTING_ACTION_EXE ?= $(TESTING_ACTION_EXE)
test: $(gotestsum) $(TESTING_ACTION_EXE)
	$(GO_TEST) $(UNIT_TEST_RACE_FLAGS) -tags testing -count=$(REPEAT_TEST) -timeout $(UNIT_TESTS_TIMEOUT) -skip '^TestIndexBlobManagerStress$$' ./...
	-$(gotestsum) tool slowest --jsonfile .tmp.unit-tests.json  --threshold 1000ms

test-index-blob-v0: GOTESTSUM_FLAGS=--format=pkgname --no-summary=output,skipped
test-index-blob-v0: $(gotestsum) $(TESTING_ACTION_EXE)
	$(GO_TEST) $(UNIT_TEST_RACE_FLAGS) -tags testing -count=$(REPEAT_TEST) -timeout $(UNIT_TESTS_TIMEOUT)  -run '^TestIndexBlobManagerStress$$' ./repo/content/indexblob/...

provider-tests-deps: $(gotestsum) $(rclone) $(MINIO_MC_PATH)

PROVIDER_TEST_TARGET=...

provider-tests: export KOPIA_PROVIDER_TEST=true
provider-tests: export RCLONE_EXE=$(rclone)
provider-tests: GOTESTSUM_FLAGS=--format=$(GOTESTSUM_FORMAT) --no-summary=skipped --jsonfile=.tmp.provider-tests.json
provider-tests: $(gotestsum) $(rclone) $(MINIO_MC_PATH)
	$(GO_TEST) $(UNIT_TEST_RACE_FLAGS) -count=$(REPEAT_TEST) -timeout 15m ./repo/blob/$(PROVIDER_TEST_TARGET)
	-$(gotestsum) tool slowest --jsonfile .tmp.provider-tests.json  --threshold 1000ms

ALLOWED_LICENSES=Apache-2.0;MIT;BSD-2-Clause;BSD-3-Clause;CC0-1.0;ISC;MPL-2.0;CC-BY-3.0;CC-BY-4.0;ODC-By-1.0;WTFPL;0BSD;Python-2.0;BSD;Unlicense

license-check: $(wwhrd) app-node-modules
	$(wwhrd) check
	(cd app && npx license-checker --summary --production --onlyAllow "$(ALLOWED_LICENSES)")

vtest: $(gotestsum)
	$(GO_TEST) -count=$(REPEAT_TEST) -short -v -timeout $(UNIT_TESTS_TIMEOUT) ./...

build-integration-test-binary:
	go build $(KOPIA_BUILD_FLAGS) $(INTEGRATION_TEST_RACE_FLAGS) -o $(KOPIA_INTEGRATION_EXE) -tags testing github.com/kopia/kopia

$(TESTING_ACTION_EXE): tests/testingaction/main.go
	go build -o $(TESTING_ACTION_EXE) -tags testing github.com/kopia/kopia/tests/testingaction

compat-tests: export KOPIA_CURRENT_EXE=$(CURDIR)/$(kopia_ui_embedded_exe)
compat-tests: export KOPIA_08_EXE=$(kopia08)
compat-tests: export KOPIA_017_EXE=$(kopia017)
compat-tests: GOTESTSUM_FLAGS=--format=testname --no-summary=skipped --jsonfile=.tmp.compat-tests.json
compat-tests: $(kopia_ui_embedded_exe) $(kopia08) $(kopia017) $(gotestsum)
	$(GO_TEST) $(TEST_FLAGS) -count=$(REPEAT_TEST) -parallel $(PARALLEL) -timeout 3600s github.com/kopia/kopia/tests/compat_test
	#  -$(gotestsum) tool slowest --jsonfile .tmp.compat-tests.json  --threshold 1000ms

endurance-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
endurance-tests: export KOPIA_LOGS_DIR=$(CURDIR)/.logs
endurance-tests: export KOPIA_TRACK_CHUNK_ALLOC=1
endurance-tests: build-integration-test-binary $(gotestsum)
	go test $(TEST_FLAGS) -count=$(REPEAT_TEST) -parallel $(PARALLEL) -timeout 3600s github.com/kopia/kopia/tests/endurance_test

recovery-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
recovery-tests: GOTESTSUM_FORMAT=testname
recovery-tests: build-integration-test-binary $(gotestsum)
	FIO_DOCKER_IMAGE=$(FIO_DOCKER_TAG) \
	$(GO_TEST) -count=$(REPEAT_TEST) github.com/kopia/kopia/tests/recovery/recovery_test $(TEST_FLAGS)

robustness-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
robustness-tests: GOTESTSUM_FORMAT=testname
robustness-tests: build-integration-test-binary $(gotestsum)
	FIO_DOCKER_IMAGE=$(FIO_DOCKER_TAG) \
	$(GO_TEST) -count=$(REPEAT_TEST) github.com/kopia/kopia/tests/robustness/robustness_test $(TEST_FLAGS)

robustness-server-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
robustness-server-tests: GOTESTSUM_FORMAT=testname
robustness-server-tests: build-integration-test-binary $(gotestsum)
	FIO_DOCKER_IMAGE=$(FIO_DOCKER_TAG) \
	$(GO_TEST) -count=$(REPEAT_TEST) github.com/kopia/kopia/tests/robustness/multiclient_test $(TEST_FLAGS)

robustness-tool-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
robustness-tool-tests: export FIO_DOCKER_IMAGE=$(FIO_DOCKER_TAG)
robustness-tool-tests: build-integration-test-binary $(gotestsum)
ifeq ($(GOOS)/$(GOARCH),linux/amd64)
	$(GO_TEST) -count=$(REPEAT_TEST) github.com/kopia/kopia/tests/tools/... github.com/kopia/kopia/tests/robustness/engine/... $(TEST_FLAGS)
endif

socket-activation-tests: export KOPIA_ORIG_EXE ?= $(KOPIA_INTEGRATION_EXE)
socket-activation-tests: export KOPIA_SERVER_EXE ?= $(CURDIR)/tests/socketactivation_test/server_wrap.sh
socket-activation-tests: export FIO_DOCKER_IMAGE=$(FIO_DOCKER_TAG)
socket-activation-tests: build-integration-test-binary $(gotestsum)
ifeq ($(GOOS),linux)
	$(GO_TEST) -count=$(REPEAT_TEST) github.com/kopia/kopia/tests/socketactivation_test $(TEST_FLAGS)
endif

stress-test: export KOPIA_STRESS_TEST=1
stress-test: export KOPIA_DEBUG_MANIFEST_MANAGER=1
stress-test: export KOPIA_LOGS_DIR=$(CURDIR)/.logs
stress-test: export KOPIA_KEEP_LOGS=1
stress-test: $(gotestsum)
	$(GO_TEST) -count=$(REPEAT_TEST) -timeout 3600s github.com/kopia/kopia/tests/stress_test
	$(GO_TEST) -count=$(REPEAT_TEST) -timeout 3600s github.com/kopia/kopia/tests/repository_stress_test

os-snapshot-tests: export KOPIA_EXE ?= $(KOPIA_INTEGRATION_EXE)
os-snapshot-tests: GOTESTSUM_FORMAT=testname
os-snapshot-tests: build-integration-test-binary $(gotestsum)
	$(GO_TEST) -count=$(REPEAT_TEST) github.com/kopia/kopia/tests/os_snapshot_test $(TEST_FLAGS)

layering-test:
ifneq ($(GOOS),windows)
	# verify that code under repo/ can only import code also under repo/ + some
	# whitelisted internal packages.
	find repo/ -name '*.go' | xargs grep "^\t\"github.com/kopia/kopia" \
	   | grep -v -e github.com/kopia/kopia/repo \
	             -e github.com/kopia/kopia/internal \
	             -e github.com/kopia/kopia/issues && exit 1 || echo repo/ layering ok
endif

htmlui-e2e-test:
	HTMLUI_E2E_TEST=1 go test -timeout 600s github.com/kopia/kopia/tests/htmlui_e2e_test -v $(TEST_FLAGS)

htmlui-e2e-test-local-htmlui-changes:
	(cd ../htmlui && npm run build)
	HTMLUI_E2E_TEST=1 HTMLUI_BUILD_DIR=$(CURDIR)/../htmlui/build go test -timeout 600s github.com/kopia/kopia/tests/htmlui_e2e_test -v $(TEST_FLAGS)

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

ci-gpg-key:
ifneq ($(GPG_KEYRING),)
	@echo "$(GPG_KEYRING)" | base64 -d | gpg --import
else
	@echo No GPG keyring
endif

ci-gcs-creds:
ifneq ($(GCS_CREDENTIALS),)
	@echo $(GCS_CREDENTIALS) | base64 -d | gzip -d | gcloud auth activate-service-account --key-file=/dev/stdin
else
	@echo No GPG credentials.
endif

RELEASE_STAGING_DIR=$(CURDIR)/.release

stage-release:
	rm -rf $(RELEASE_STAGING_DIR)
	mkdir -p $(RELEASE_STAGING_DIR)

	# copy all dist files to a staging directory
	find dist -type f -exec cp -v {} $(RELEASE_STAGING_DIR) \;

	# sign RPMs
	find $(RELEASE_STAGING_DIR) -type f -name '*.rpm' -exec rpm --define "%_gpg_name Kopia Builder" --addsign {} \;

	# regenerate checksums file and sign it
	(cd $(RELEASE_STAGING_DIR) && sha256sum * > checksums.txt)
	cat $(RELEASE_STAGING_DIR)/checksums.txt
	gpg --output $(RELEASE_STAGING_DIR)/checksums.txt.sig --detach-sig $(RELEASE_STAGING_DIR)/checksums.txt

ifeq ($(IS_PULL_REQUEST),false)
ifneq ($(CI_TAG),)
GH_RELEASE_REPO=$(GITHUB_REPOSITORY)
GH_RELEASE_FLAGS=
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

generate-change-log: $(gitchglog)
ifeq ($(CI_TAG),)
	$(gitchglog) --next-tag latest latest > dist/change_log.md
else
	$(gitchglog) $(CI_TAG) > dist/change_log.md
endif

push-github-release:
ifneq ($(GH_RELEASE_REPO),)
	@echo Creating Github Release $(GH_RELEASE_NAME) in $(GH_RELEASE_REPO) with flags $(GH_RELEASE_FLAGS)
	gh --repo $(GH_RELEASE_REPO) release view $(GH_RELEASE_NAME) || gh --repo $(GH_RELEASE_REPO) release create -F dist/change_log.md $(GH_RELEASE_FLAGS) $(GH_RELEASE_NAME)
	gh --repo $(GH_RELEASE_REPO) release upload $(GH_RELEASE_NAME) $(RELEASE_STAGING_DIR)/*
else
	@echo Not creating Github Release
endif

publish-apt:
	$(CURDIR)/tools/apt-publish.sh $(RELEASE_STAGING_DIR)

publish-rpm:
	$(CURDIR)/tools/rpm-publish.sh $(RELEASE_STAGING_DIR)

publish-homebrew:
	$(CURDIR)/tools/homebrew-publish.sh $(RELEASE_STAGING_DIR) $(KOPIA_VERSION_NO_PREFIX)

publish-scoop:
	$(CURDIR)/tools/scoop-publish.sh $(RELEASE_STAGING_DIR) $(KOPIA_VERSION_NO_PREFIX)

publish-docker:
ifneq ($(DOCKERHUB_TOKEN),)
	@echo $(DOCKERHUB_TOKEN) | docker login --username $(DOCKERHUB_USERNAME) --password-stdin
	$(CURDIR)/tools/docker-publish.sh $(CURDIR)/dist_binaries
else
	@echo DOCKERHUB_TOKEN is not set.
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
