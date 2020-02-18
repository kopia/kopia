#!/usr/bin/env sh
set -o nounset
set -o errexit

echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker push "${1-kanisterio/kopia}"
