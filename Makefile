all: test lint

travis: test upload-coverage

setup:
	GO111MODULE=off go get github.com/mattn/goveralls
	GO111MODULE=off go get -u gopkg.in/alecthomas/gometalinter.v2
	GO111MODULE=off gometalinter.v2 --install

travis-setup:
	GO111MODULE=off go get github.com/mattn/goveralls

lint:
	gometalinter.v2 ./...

test:
	GO111MODULE=on go test -tags test -count=1 -coverprofile=raw.cov --coverpkg ./... -timeout 90s ./...
	grep -v testing/ raw.cov > tmp.cov

upload-coverage:
	goveralls -service=travis-ci -coverprofile=tmp.cov

coverage-html:
	go tool cover -html=tmp.cov

godoc:
	godoc -http=:33333
