FROM golang:1.19.1-alpine as builder

RUN apk add --no-cache git build-base

RUN cd /go/src && git clone https://github.com/kastenhq/kopia

WORKDIR /go/src/kopia

ARG kopiaCommit="boring_kopia"

RUN git checkout "${kopiaCommit}"


RUN GO111MODULE=on GO_EXTLINK_ENABLED=0 GOOS=linux GOARCH=amd64 GOEXPERIMENT=boringcrypto go install \
  -ldflags="-X github.com/kopia/kopia/repo.BuildVersion=$(git show --no-patch --format='%cs-%h') -X github.com/kopia/kopia/repo.BuildInfo=$(git show --no-patch --format='%cI-%H')-${kopiaCommit}" .

FROM alpine

WORKDIR /kopia

# Add CA certs
RUN apk add --no-cache --verbose ca-certificates go libc6-compat && \
  rm -rf /var/cache/apk/* && \
  adduser -D kopia && addgroup kopia kopia && \
  chown kopia /kopia

USER kopia:kopia

ENTRYPOINT [ "/bin/kopia" ]

COPY --from=builder --chown=kopia:kopia /go/bin/kopia /bin/kopia

ARG imageVersion="unknown"
# TODO: replace with build date + git commit info
LABEL imageVersion="${imageVersion}"
LABEL kopiaCommit="${kopiaCommit}"