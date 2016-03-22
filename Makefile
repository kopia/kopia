build:
	go install github.com/kopia/kopia/cmd/kopia

deps:
	go get -u -t -v github.com/kopia/kopia/...

dev-deps:
	go get -u golang.org/x/tools/cmd/gorename
	go get -u github.com/golang/lint/golint
	go get -u golang.org/x/tools/cmd/oracle
	go get -u github.com/nsf/gocode

test:
	go test -timeout 30s github.com/kopia/kopia/...
