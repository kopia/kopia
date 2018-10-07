all: install install-examples test lint vet integration-tests

install:
	go install github.com/kopia/kopia

install-examples:
	go install github.com/kopia/kopia/examples/repository

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
	go tool vet -all .

deps:
	GOOS=linux GOARCH=amd64 go get -t -v github.com/kopia/kopia/...
	GOOS=darwin GOARCH=amd64 go get -t -v github.com/kopia/kopia/...
	GOOS=windows GOARCH=amd64 go get -t -v github.com/kopia/kopia/...

travis-setup: deps dev-deps

travis-release: test lint vet verify-release integration-tests long-test upload-coverage

verify-release:
	curl -sL https://git.io/goreleaser | bash /dev/stdin --skip-publish --rm-dist --snapshot

upload-coverage:
	$(GOPATH)/bin/goveralls -service=travis-ci -coverprofile=tmp.cov

dev-deps:
	go get -u golang.org/x/tools/cmd/gorename
	go get -u github.com/golang/lint/golint
	go get -u golang.org/x/tools/cmd/guru
	go get -u github.com/nsf/gocode
	go get -u github.com/rogpeppe/godef
	go get -u github.com/lukehoban/go-outline
	go get -u github.com/newhook/go-symbols
	go get -u github.com/sqs/goreturns
	go get -u gopkg.in/alecthomas/gometalinter.v2
	go get github.com/mattn/goveralls
	gometalinter.v2 --install

test: install
	go test -count=1 -coverprofile=tmp.cov -short -timeout 90s github.com/kopia/kopia/...

vtest:
	go test -count=1 -short -v -timeout 90s github.com/kopia/kopia/...

long-test: install
	go test -count=1 -timeout 90s github.com/kopia/kopia/...

integration-tests:
	go build -o dist/integration/kopia github.com/kopia/kopia
	KOPIA_EXE=$(CURDIR)/dist/integration/kopia go test -count=1 -timeout 90s -v github.com/kopia/kopia/tests/end_to_end_test

stress-test:
	KOPIA_LONG_STRESS_TEST=1 go test -count=1 -timeout 200s github.com/kopia/kopia/repo/tests/stress_test
	go test -count=1 -timeout 200s github.com/kopia/kopia/repo/tests/repository_stress_test

godoc:
	godoc -http=:33333

coverage:
	go test --coverprofile tmp.cov github.com/kopia/kopia/...
	go tool cover -html=tmp.cov

coverage-repo:
	go test --coverprofile tmp.cov github.com/kopia/kopia/repo/...
	go tool cover -html=tmp.cov
