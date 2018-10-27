all: test lint

travis: test lint upload-coverage

setup:
	GO111MODULE=off go get github.com/mattn/goveralls
	GO111MODULE=off go get -u gopkg.in/alecthomas/gometalinter.v2
	GO111MODULE=off gometalinter.v2 --install

lint:
	GO111MODULE=on gometalinter.v2 ./...

test:
	GO111MODULE=on go test -count=1 -coverprofile=tmp.cov --coverpkg ./... -timeout 90s ./...

upload-coverage:
	goveralls -service=travis-ci -coverprofile=tmp.cov

coverage-html:
	go tool cover -html=tmp.cov

godoc:
	godoc -http=:33333
