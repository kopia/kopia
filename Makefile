BUILD_VERSION ?= $(shell git describe --dirty)
BUILD_INFO ?= $(USER)@$(shell hostname -s)_$(shell date +%Y%m%d_%H%M%S)
RELEASE_VERSION ?= $(BUILD_VERSION)-$(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = kopia-$(RELEASE_VERSION)
LDARGS="-X main.buildVersion=$(BUILD_VERSION) -X main.buildInfo=$(BUILD_INFO)"
RELEASE_TMP_DIR = .release

all: install test lint vet

install:
	@echo Building version: $(BUILD_INFO) / $(BUILD_VERSION)
	go install -ldflags $(LDARGS) github.com/kopia/kopia/cmd/kopia

build:
	go build github.com/kopia/kopia/...

play:
	go run cmd/playground/main.go

lint:
	golint github.com/kopia/kopia/...

vet:
	go tool vet -all .

deps:
	go get -t -v github.com/kopia/kopia/...

release:
	rm -rf $(RELEASE_TMP_DIR)
	mkdir -p $(RELEASE_TMP_DIR)/$(RELEASE_NAME)/bin
	go build -o $(RELEASE_TMP_DIR)/$(RELEASE_NAME)/bin/kopia$(EXE_SUFFIX) -ldflags $(LDARGS) github.com/kopia/kopia/cmd/kopia
	cp README.md LICENSE $(RELEASE_TMP_DIR)/$(RELEASE_NAME)
	(cd $(RELEASE_TMP_DIR) && tar -cvzf $(RELEASE_NAME).tar.gz $(RELEASE_NAME)/)
	mv $(RELEASE_TMP_DIR)/$(RELEASE_NAME).tar.gz .

travis-setup: deps dev-deps

travis-release: all
	GOARCH=amd64 GOOS=windows EXE_SUFFIX=.exe make release
	GOARCH=amd64 GOOS=linux make release
	GOARCH=amd64 GOOS=darwin make release
	GOARCH=arm GOOS=linux make release
	rm -rf $(RELEASE_TMP_DIR)

dev-deps:
	go get -u golang.org/x/tools/cmd/gorename
	go get -u github.com/golang/lint/golint
	go get -u golang.org/x/tools/cmd/oracle
	go get -u github.com/nsf/gocode
	go get -u github.com/rogpeppe/godef
	go get -u github.com/lukehoban/go-outline
	go get -u github.com/newhook/go-symbols
	go get -u github.com/sqs/goreturns

test: install
	go test -timeout 30s github.com/kopia/kopia/...

vtest:
	go test -v -timeout 30s github.com/kopia/kopia/...

godoc:
	godoc -http=:33333

coverage:
	./coverage.sh
