#!/bin/bash
set -e
dist_dir=$1
ver=$2

target_repo=kopia/scoop-bucket
source_repo=kopia/kopia

if [ "$CI_TAG" == "" ]; then
  echo Not publishing Scoop package because CI_TAG is not set.
  exit 0
fi

if [ "$GITHUB_TOKEN" == "" ]; then
  echo Not publishing Scoop package because GITHUB_TOKEN is not set.
  exit 0
fi

if [ "$REPO_OWNER/$GITHUB_REF" != "kopia/refs/heads/master" ]; then
  echo Not publishing Scoop package on $REPO_OWNER/$GITHUB_REF
  exit 0
fi

echo Publishing Scoop version $source_repo version $ver to $target_repo from $dist_dir...

HASH_WINDOWS_AMD64=$(sha256sum $dist_dir/kopia-$ver-windows-x64.zip | cut -f 1 -d " ")
tmpdir=$(mktemp -d)
git clone https://$GITHUB_TOKEN@github.com/$target_repo.git $tmpdir

cat tools/scoop-kopia.json.template | \
   sed "s/VERSION/$ver/g" | \
   sed "s!SOURCE_REPO!$source_repo!g" | \
   sed "s/HASH_WINDOWS_AMD64/$HASH_WINDOWS_AMD64/g" > $tmpdir/kopia.json

(cd $tmpdir && git add kopia.json && git -c "user.name=Kopia Builder" -c "user.email=builder@kopia.io" commit -m "Scoop update for kopia version v$ver" && git push)
rm -rf "$tmpdir"
