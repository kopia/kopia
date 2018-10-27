all: test lint

travis: test lint upload-coverage

setup:
	go get github.com/mattn/goveralls
	go get -u gopkg.in/alecthomas/gometalinter.v2
	gometalinter.v2 --install

lint:
	gometalinter.v2 ./...

test:
	go test -count=1 -coverprofile=tmp.cov --coverpkg ./... -timeout 90s ./...

upload-coverage:
	goveralls -service=travis-ci -coverprofile=tmp.cov

coverage-html:
	go tool cover -html=tmp.cov

godoc:
	godoc -http=:33333
