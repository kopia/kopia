BUILD_VERSION ?= $(shell git describe --dirty)
BUILD_INFO ?= $(USER)@$(shell hostname -s)_$(shell date +%Y%m%d_%H%M%S)
RELEASE_SUFFIX ?= $(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_VERSION ?= $(BUILD_VERSION)-$(RELEASE_SUFFIX)
RELEASE_NAME = kopia-$(RELEASE_VERSION)
LDARGS="-X main.buildVersion=$(BUILD_VERSION)"
RELEASE_TMP_DIR = $(CURDIR)/.release
RELEASES_OUT_DIR = $(CURDIR)/.releases
ZIP ?= 0

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
ifeq ($(GOOS), windows)
	(cd $(RELEASE_TMP_DIR) && zip -r $(RELEASES_OUT_DIR)/$(RELEASE_NAME).zip $(RELEASE_NAME)/)
else
	(cd $(RELEASE_TMP_DIR) && tar -cvzf $(RELEASES_OUT_DIR)/$(RELEASE_NAME).tar.gz $(RELEASE_NAME)/)
endif

travis-setup: deps dev-deps

travis-release:
	mkdir -p $(RELEASES_OUT_DIR)
	GOARCH=386 GOOS=windows EXE_SUFFIX=.exe RELEASE_SUFFIX=windows-x86 make release
	GOARCH=amd64 GOOS=windows EXE_SUFFIX=.exe RELEASE_SUFFIX=windows-x64 make release
	GOARCH=386 GOOS=linux RELEASE_SUFFIX=linux-x86 make release
	GOARCH=amd64 GOOS=linux RELEASE_SUFFIX=linux-x64 make release
	GOARCH=amd64 GOOS=darwin RELEASE_SUFFIX=macosx-x64 make release
	GOARCH=arm GOOS=linux RELEASE_SUFFIX=linux-arm make release
	rm -rf $(RELEASE_TMP_DIR)
	(cd $(RELEASES_OUT_DIR) && sha256sum kopia-* > CHECKSUM.txt)
	(cd ../../.. && find  -name .git | xargs -Izzz /bin/bash -c "(cd zzz && echo -n 'zzz: ' && git describe --always --long --abbrev=40)") | sort > $(RELEASES_OUT_DIR)/BUILD_INFO.txt

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
