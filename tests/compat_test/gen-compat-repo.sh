#!/bin/sh
set -e
if [[ $CI_TAG == "" ]]; then
    echo CI_TAG must be set.
    exit 0
fi

if [[ $KOPIA_EXE == "" ]]; then
    echo KOPIA_EXE must be set.
    exit 0
fi

OUTPUT_DIR="compat-test/$CI_TAG"
rm -rf "$OUTPUT_DIR"

mkdir -pv "$OUTPUT_DIR"
TMP_DIR=$(mktemp -d)
mkdir -pv $TMP_DIR/{config,repo}
export KOPIA_PASSWORD=long-term-test 
KOPIA_ARGS="--config-file=$TMP_DIR/config/kopia.config"
$KOPIA_EXE $KOPIA_ARGS repository create filesystem --path "$TMP_DIR/repo"

$KOPIA_EXE $KOPIA_ARGS snap create repo
$KOPIA_EXE $KOPIA_ARGS snap create cli
$KOPIA_EXE $KOPIA_ARGS snap create site

$KOPIA_EXE $KOPIA_ARGS snap create repo
$KOPIA_EXE $KOPIA_ARGS snap create cli
$KOPIA_EXE $KOPIA_ARGS snap create site
(cd $TMP_DIR && tar cf - "repo/") > "$OUTPUT_DIR/repo.tar"
cp dist/*.tar.gz "$OUTPUT_DIR"
rm -rf "$TMP_DIR"
gsutil cp -rv $OUTPUT_DIR gs://kopia-compat-test/