FROM alpine
ARG TARGETARCH
RUN apk add --no-cache --verbose ca-certificates && adduser -D kopia && addgroup kopia kopia
USER kopia:kopia
ENTRYPOINT ["/kopia"]
COPY bin-${TARGETARCH}/kopia /
