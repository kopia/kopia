all: install test lint vet

install:
	@echo Building version: `git describe --dirty`
	go install -ldflags "-X main.buildVersion=`git describe --dirty` -X main.buildInfo=$(USER)@`hostname -s`/`date +%Y%m%d_%H%M%S`" github.com/kopia/kopia/cmd/kopia

build:
	go build github.com/kopia/kopia/...

play:
	go run cmd/playground/main.go

lint:
	golint github.com/kopia/kopia/...

vet:
	go tool vet -all .

deps:
	go get -t -v -ldflags "-X main.buildVersion=`git describe --dirty` -X main.buildInfo=$(USER)@`hostname -s`/`date +%Y%m%d_%H%M%S`" github.com/kopia/kopia/...

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
