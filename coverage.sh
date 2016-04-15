echo mode: set > combined.cov
for p in `go list github.com/kopia/kopia/...`; do
    go test --coverprofile tmp.cov $p
    grep -v "mode: " tmp.cov >> combined.cov
    rm tmp.cov
done
go tool cover -html=combined.cov
rm combined.cov
