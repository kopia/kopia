FROM gcr.io/distroless/static:nonroot
ARG TARGETARCH

# allow users to mount /app/config, /app/logs and /app/cache respectively
ENV KOPIA_CONFIG_PATH=/app/config/repository.config
ENV KOPIA_LOG_DIR=/app/logs
ENV KOPIA_CACHE_DIRECTORY=/app/cache

# this requires repository password to be passed via KOPIA_PASSWORD environment.
ENV KOPIA_PERSIST_CREDENTIALS_ON_CONNECT=false
ENV KOPIA_CHECK_FOR_UPDATES=false

# this creates directories writable by the current user
WORKDIR /app

COPY bin-${TARGETARCH}/kopia .

ENTRYPOINT ["/app/kopia"]
