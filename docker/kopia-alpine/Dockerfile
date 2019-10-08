FROM golang:1.16-alpine3.13 AS builder

RUN adduser -D kopia && addgroup kopia kopia
USER kopia:kopia

COPY --chown=kopia . /kopia
WORKDIR /kopia

ARG imageVersion="unknown"
ARG kopiaCommit="unknown"

LABEL imageVersion="${imageVersion}"
LABEL kopiaCommit="${kopiaCommit}"

# Build static executable
RUN GO111MODULE=on CGO_ENABLED=0 GO_EXTLINK_ENABLED=0 go build \
  -ldflags="-s -w -X github.com/kopia/kopia/repo.BuildVersion=${imageVersion} -X github.com/kopia/kopia/repo.BuildInfo=${kopiaCommit}" \
  .

FROM alpine:3.13

WORKDIR /kopia

# Add CA certs
RUN apk add --no-cache --verbose ca-certificates && \
  rm -rf /var/cache/apk/* && \
  adduser -D kopia && addgroup kopia kopia && \
  chown kopia /kopia

USER kopia:kopia

ENTRYPOINT [ "/kopia/kopia" ]

COPY --from=builder --chown=kopia:kopia /kopia/kopia /kopia/kopia
