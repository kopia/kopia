LINTER_TOOL=.tools/bin/golangci-lint
GOVERALLS_TOOL=.tools/bin/goveralls

all: test lint

travis: build-all test upload-coverage

setup:
	GO111MODULE=off go get github.com/mattn/goveralls
	GO111MODULE=off go get -u gopkg.in/alecthomas/gometalinter.v2
	GO111MODULE=off gometalinter.v2 --install

lint: $(LINTER_TOOL)
	$(LINTER_TOOL) run

$(LINTER_TOOL):
	mkdir -p .tools
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b .tools/bin/ v1.16.0

$(GOVERALLS_TOOL):
	mkdir -p .tools
	GO111MODULE=off GOPATH=$(CURDIR)/.tools go get github.com/mattn/goveralls

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

test:
	GO111MODULE=on go test -tags test -count=1 -coverprofile=raw.cov --coverpkg ./... -timeout 90s ./...
	grep -v testing/ raw.cov > tmp.cov

upload-coverage:
	$(GOVERALLS_TOOL) -service=travis-ci -coverprofile=tmp.cov

coverage-html:
	go tool cover -html=tmp.cov

godoc:
	godoc -http=:33333
