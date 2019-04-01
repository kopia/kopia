COVERAGE_PACKAGES=./repo/...,./fs/...,./snapshot/...

all: install test lint vet integration-tests

install:
	go install github.com/kopia/kopia

install-race:
	go install -race github.com/kopia/kopia

build:
	go build github.com/kopia/kopia/...

escape-analysis:
	go build -gcflags '-m -l' github.com/kopia/kopia/...

play:
	go run cmd/playground/main.go

lint:
	gometalinter.v2 ./...

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

deps:
	GOOS=linux GOARCH=amd64 go get -t -v github.com/kopia/kopia/...
	GOOS=darwin GOARCH=amd64 go get -t -v github.com/kopia/kopia/...
	GOOS=windows GOARCH=amd64 go get -t -v github.com/kopia/kopia/...

travis-setup: deps dev-deps

travis-release: test-with-coverage lint vet verify-release integration-tests upload-coverage

verify-release:
	curl -sL https://git.io/goreleaser | bash /dev/stdin --skip-publish --rm-dist --snapshot

upload-coverage:
	$(GOPATH)/bin/goveralls -service=travis-ci -coverprofile=tmp.cov

dev-deps:
	GO111MODULE=off go get -u golang.org/x/tools/cmd/gorename
	GO111MODULE=off go get -u golang.org/x/tools/cmd/guru
	GO111MODULE=off go get -u github.com/nsf/gocode
	GO111MODULE=off go get -u github.com/rogpeppe/godef
	GO111MODULE=off go get -u github.com/lukehoban/go-outline
	GO111MODULE=off go get -u github.com/newhook/go-symbols
	GO111MODULE=off go get -u github.com/sqs/goreturns
	GO111MODULE=off go get -u gopkg.in/alecthomas/gometalinter.v2
	GO111MODULE=off go get github.com/mattn/goveralls
	GO111MODULE=off gometalinter.v2 --install

test-with-coverage: install
	go test -count=1 -coverprofile=tmp.cov --coverpkg $(COVERAGE_PACKAGES) -timeout 90s github.com/kopia/kopia/...

test-with-coverage-pkgonly: install
	go test -count=1 -coverprofile=tmp.cov -timeout 90s github.com/kopia/kopia/...

test: install
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
