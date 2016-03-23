all: lint vet build

build:
	go build github.com/kopia/kopia/...

lint:
	golint github.com/kopia/kopia/...

vet:
	go vet github.com/kopia/kopia/...

deps:
	go get -u -t -v github.com/kopia/kopia/...

dev-deps:
	go get -u golang.org/x/tools/cmd/gorename
	go get -u github.com/golang/lint/golint
	go get -u golang.org/x/tools/cmd/oracle
	go get -u github.com/nsf/gocode

test:
	go test -timeout 30s github.com/kopia/kopia/...
