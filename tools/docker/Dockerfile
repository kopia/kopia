FROM alpine
ARG TARGETARCH

# install CA certificates and create user/group 'kopia' with IDs 51515:51515
RUN apk add --no-cache --verbose ca-certificates && \
  addgroup -g 51515 kopia && \
  adduser -u 51515 -H -D -G kopia kopia && \
  mkdir -p /logs /cache /config && \
  chown kopia:kopia /logs /cache /config

USER kopia:kopia
ENTRYPOINT ["/kopia"]

# allow users to mount /config, /logs and /cache respectively
ENV KOPIA_CONFIG_PATH=/config/repository.config
ENV KOPIA_LOG_DIR=/logs
ENV KOPIA_CACHE_DIRECTORY=/cache

# this requires repository password to be passed via KOPIA_PASSWORD environment.
ENV KOPIA_PERSIST_CREDENTIALS_ON_CONNECT=false
ENV KOPIA_CHECK_FOR_UPDATES=false

COPY bin-${TARGETARCH}/kopia /
