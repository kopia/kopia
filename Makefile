COVERAGE_PACKAGES=./repo/...,./fs/...,./snapshot/...
LINTER_TOOL=.tools/bin/golangci-lint
GOVERALLS_TOOL=.tools/bin/goveralls

all: test lint vet integration-tests

build:
	go build github.com/kopia/kopia/...

escape-analysis:
	go build -gcflags '-m -l' github.com/kopia/kopia/...

play:
	go run cmd/playground/main.go

lint: $(LINTER_TOOL)
	$(LINTER_TOOL) run

vet:
	go vet -all .

build-linux-amd64:
	CGO_ENABLED=0 GO111MODULE=on GOARCH=amd64 GOOS=linux go build ./...

build-windows-amd64:
	CGO_ENABLED=0 GO111MODULE=on GOARCH=amd64 GOOS=windows go build ./...

build-darwin-amd64:
	CGO_ENABLED=0 GO111MODULE=on GOARCH=amd64 GOOS=darwin go build ./...

build-linux-arm:
	CGO_ENABLED=0 GO111MODULE=on GOARCH=arm GOOS=linux go build ./...

build-linux-arm64:
	CGO_ENABLED=0 GO111MODULE=on GOARCH=arm64 GOOS=linux go build ./...

build-all: build-linux-amd64 build-windows-amd64 build-darwin-amd64 build-linux-arm build-linux-arm64

$(LINTER_TOOL):
	mkdir -p .tools
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b .tools/bin/ v1.16.0

$(GOVERALLS_TOOL):
	mkdir -p .tools
	GO111MODULE=off GOPATH=$(CURDIR)/.tools go get github.com/mattn/goveralls

travis-release: test-with-coverage lint vet verify-release integration-tests upload-coverage

verify-release:
	curl -sL https://git.io/goreleaser | bash /dev/stdin --skip-publish --rm-dist --snapshot

upload-coverage: $(GOVERALLS_TOOL)
	$(GOVERALLS_TOOL) -service=travis-ci -coverprofile=tmp.cov

dev-deps:
	GO111MODULE=off go get -u golang.org/x/tools/cmd/gorename
	GO111MODULE=off go get -u golang.org/x/tools/cmd/guru
	GO111MODULE=off go get -u github.com/nsf/gocode
	GO111MODULE=off go get -u github.com/rogpeppe/godef
	GO111MODULE=off go get -u github.com/lukehoban/go-outline
	GO111MODULE=off go get -u github.com/newhook/go-symbols
	GO111MODULE=off go get -u github.com/sqs/goreturns
	
test-with-coverage:
	go test -count=1 -coverprofile=tmp.cov --coverpkg $(COVERAGE_PACKAGES) -timeout 90s github.com/kopia/kopia/...

test-with-coverage-pkgonly:
	go test -count=1 -coverprofile=tmp.cov -timeout 90s github.com/kopia/kopia/...

test:
	go test -count=1 -timeout 90s github.com/kopia/kopia/...

vtest:
	go test -count=1 -short -v -timeout 90s github.com/kopia/kopia/...

integration-tests:
	go build -o dist/integration/kopia github.com/kopia/kopia
	KOPIA_EXE=$(CURDIR)/dist/integration/kopia go test -count=1 -timeout 90s github.com/kopia/kopia/tests/end_to_end_test

stress-test:
	KOPIA_LONG_STRESS_TEST=1 go test -count=1 -timeout 200s github.com/kopia/repo/tests/stress_test
	go test -count=1 -timeout 200s github.com/kopia/repo/tests/repository_stress_test

godoc:
	godoc -http=:33333

coverage: test-with-coverage coverage-html

coverage-html:
	go tool cover -html=tmp.cov
