FROM gcr.io/distroless/static:nonroot
ARG TARGETARCH

# allow users to mount /app/config, /app/logs and /app/cache, /app/rclone respectively
ENV KOPIA_CONFIG_PATH=/app/config/repository.config
ENV KOPIA_LOG_DIR=/app/logs
ENV KOPIA_CACHE_DIRECTORY=/app/cache

# allow user to mount ~/.config/rclone to /app/rclone
ENV RCLONE_CONFIG=/app/rclone/rclone.conf

# this requires repository password to be passed via KOPIA_PASSWORD environment.
ENV KOPIA_PERSIST_CREDENTIALS_ON_CONNECT=false
ENV KOPIA_CHECK_FOR_UPDATES=false

# this creates directories writable by the current user
WORKDIR /app

ENV PATH=/bin

COPY bin-${TARGETARCH}/kopia .
COPY bin-${TARGETARCH}/rclone /bin/rclone

ENTRYPOINT ["/app/kopia"]
