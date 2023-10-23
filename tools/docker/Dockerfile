FROM		ubuntu:jammy

ARG 		TARGETARCH

ENV 		DEBIAN_FRONTEND="noninteractive" \
		    TERM="xterm-256color" \
		    LC_ALL="C.UTF-8" \
		    KOPIA_CONFIG_PATH=/app/config/repository.config \
		    KOPIA_LOG_DIR=/app/logs \
		    KOPIA_CACHE_DIRECTORY=/app/cache \
		    RCLONE_CONFIG=/app/rclone/rclone.conf \
		    KOPIA_PERSIST_CREDENTIALS_ON_CONNECT=false \
		    KOPIA_CHECK_FOR_UPDATES=false

RUN 		apt-get update && \
		    apt-get upgrade -y --with-new-pkgs && \
		    apt-get install -y --no-install-recommends fuse3 ca-certificates curl tzdata && \
		    apt-get clean autoclean -y && \
    		apt-get autoremove -y && \
    		rm -rf /var/lib/apt/* /var/lib/cache/* /var/lib/log/* \
		    /var/tmp/* /usr/share/doc/ /usr/share/man/ /usr/share/locale/ \
		    /root/.cache /root/.local /root/.gnupg /root/.config /tmp/*
	
WORKDIR /app

COPY 		bin-${TARGETARCH}/kopia /bin/kopia
COPY 		bin-${TARGETARCH}/rclone /bin/rclone
  
ENTRYPOINT 	["/bin/kopia"]
