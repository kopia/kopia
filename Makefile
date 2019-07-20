COVERAGE_PACKAGES=./repo/...,./fs/...,./snapshot/...
GOLANGCI_LINT_VERSION=v1.17.1
LINTER_TOOL=.tools/bin/golangci-lint
GOVERALLS_TOOL=.tools/bin/goveralls
GO_TEST=go test

-include ./Makefile.local.mk

all: test lint vet integration-tests

build:
	go build github.com/kopia/kopia/...

escape-analysis:
	go build -gcflags '-m -l' github.com/kopia/kopia/...

play:
	go run cmd/playground/main.go

lint: $(LINTER_TOOL)
	$(LINTER_TOOL) --deadline 180s run | tee .linterr.txt

vet:
	go vet -all .

build-linux-amd64:
	CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build ./...

build-windows-amd64:
	CGO_ENABLED=0 GOARCH=amd64 GOOS=windows go build ./...

build-darwin-amd64:
	CGO_ENABLED=0 GOARCH=amd64 GOOS=darwin go build ./...

build-linux-arm:
	CGO_ENABLED=0 GOARCH=arm GOOS=linux go build ./...

build-linux-arm64:
	CGO_ENABLED=0 GOARCH=arm64 GOOS=linux go build ./...

build-all: build-linux-amd64 build-windows-amd64 build-darwin-amd64 build-linux-arm build-linux-arm64

$(LINTER_TOOL):
	mkdir -p .tools
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b .tools/bin/ $(GOLANGCI_LINT_VERSION)

$(GOVERALLS_TOOL):
	mkdir -p .tools
	GO111MODULE=off GOPATH=$(CURDIR)/.tools go get github.com/mattn/goveralls

travis-setup: travis-install-gpg-key travis-install-test-credentials
	go mod download

website:
	$(MAKE) -C site

travis-release: test-with-coverage lint vet verify-release integration-tests upload-coverage website stress-test

verify-release:
	curl -sL https://git.io/goreleaser | bash /dev/stdin --skip-publish --skip-sign --rm-dist --snapshot 

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
	$(GO_TEST) -count=1 -coverprofile=tmp.cov --coverpkg $(COVERAGE_PACKAGES) -timeout 90s github.com/kopia/kopia/...

test-with-coverage-pkgonly:
	$(GO_TEST) -count=1 -coverprofile=tmp.cov -timeout 90s github.com/kopia/kopia/...

test:
	$(GO_TEST) -count=1 -timeout 90s github.com/kopia/kopia/...

vtest:
	$(GO_TEST) -count=1 -short -v -timeout 90s github.com/kopia/kopia/...

dist-binary:
	go build -o dist/integration/kopia github.com/kopia/kopia

integration-tests: dist-binary
	KOPIA_EXE=$(CURDIR)/dist/integration/kopia $(GO_TEST) -v -count=1 -timeout 90s github.com/kopia/kopia/tests/end_to_end_test

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
	@echo Installing GPG key...
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in kopia.gpg.enc -out /tmp/kopia.gpg -d
	gpg --import /tmp/kopia.gpg
	
travis-install-test-credentials:
	@echo Installing test credentials...
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in tests/credentials/gcs/test_service_account.json.enc -out repo/blob/gcs/test_service_account.json -d
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in tests/credentials/sftp/id_kopia.enc -out repo/blob/sftp/id_kopia -d
	openssl aes-256-cbc -K "$(encrypted_fa1db4b894bb_key)" -iv "$(encrypted_fa1db4b894bb_iv)" -in tests/credentials/sftp/known_hosts.enc -out repo/blob/sftp/known_hosts -d

else

travis-install-gpg-key:
	@echo Not installing GPG key.

travis-install-test-credentials:
	@echo Not installing test credentials.

endif