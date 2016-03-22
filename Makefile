build:
	go install github.com/kopia/kopia/cmd/kopia

deps:
	go get -u -t -v github.com/kopia/kopia/...

test:
	go test -timeout 30s github.com/kopia/kopia/...
