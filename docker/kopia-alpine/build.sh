#!/usr/bin/env sh

set -o errexit
set -o nounset
set -o xtrace

readonly COMMIT_TAG=$(git rev-parse --short=7 master)
readonly DOCKER_DIR=docker/kopia-alpine
readonly IMAGE_TYPE=alpine
readonly IMAGE_VERSION="$(date +%Y%m%d-%H%M%S)-$(git describe --long --candidates=1 --always master)"
readonly REPO=${1-kanisterio/kopia}
readonly TAG="alpine-${IMAGE_VERSION}"


REPO_DIR=$(realpath --logical --canonicalize-existing $(dirname "${0}")/../..)
cd "${REPO_DIR}"

docker build \
    --label "imageType=${IMAGE_TYPE}" \
    --label "buildCommit=$(git rev-parse --short=7 HEAD)" \
    --build-arg "imageVersion=${IMAGE_VERSION}" \
    --build-arg "kopiaCommit=$(git rev-parse master)" \
    --tag "${REPO}:${IMAGE_TYPE}" \
    --tag "${REPO}:${IMAGE_TYPE}-${COMMIT_TAG}" \
    --tag "${REPO}:${IMAGE_TYPE}-${IMAGE_VERSION}" \
    --file "${DOCKER_DIR}/Dockerfile" .

echo "Build tag: ${IMAGE_TYPE}-${COMMIT_TAG}"
echo "Run with: docker run --rm -it ${REPO}:${IMAGE_TYPE}-${COMMIT_TAG}"
