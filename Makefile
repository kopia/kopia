all: install test lint vet

install:
	go install github.com/kopia/kopia/cmd/kopia

build:
	go build github.com/kopia/kopia/...

play:
	go run cmd/playground/main.go

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

vtest:
	go test -v -timeout 30s github.com/kopia/kopia/...

doc:
	godoc -http=:33333

coverage:
	./coverage.sh
