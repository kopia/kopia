#!/usr/bin/env bash
set -e
dist_dir=$1
ver=$2

target_repo=$REPO_OWNER/homebrew-kopia
source_repo=$REPO_OWNER/kopia

if [ "$CI_TAG" == "" ]; then
    target_repo=$REPO_OWNER/homebrew-test-builds
    source_repo=$REPO_OWNER/kopia-test-builds
fi

if [ "$GITHUB_TOKEN" == "" ]; then
  echo Not publishing Homebrew package because GITHUB_TOKEN is not set.
  exit 0
fi

echo Publishing Homebrew version $source_repo version $ver to $target_repo from $dist_dir...

HASH_MAC_AMD64=$(sha256sum $dist_dir/kopia-$ver-macOS-x64.tar.gz | cut -f 1 -d " ")
HASH_MAC_ARM64=$(sha256sum $dist_dir/kopia-$ver-macOS-arm64.tar.gz | cut -f 1 -d " ")
HASH_LINUX_AMD64=$(sha256sum $dist_dir/kopia-$ver-linux-x64.tar.gz | cut -f 1 -d " ")
HASH_LINUX_ARM64=$(sha256sum $dist_dir/kopia-$ver-linux-arm64.tar.gz | cut -f 1 -d " ")
HASH_LINUX_ARM=$(sha256sum $dist_dir/kopia-$ver-linux-arm.tar.gz | cut -f 1 -d " ")
tmpdir=$(mktemp -d)
git clone https://$GITHUB_TOKEN@github.com/$target_repo.git $tmpdir

cat tools/kopia-homebrew.rs.template | \
   sed "s/VERSION/$ver/g" | \
   sed "s!SOURCE_REPO!$source_repo!g" | \
   sed "s/HASH_MAC_AMD64/$HASH_MAC_AMD64/g" | \
   sed "s/HASH_MAC_ARM64/$HASH_MAC_ARM64/g" | \
   sed "s/HASH_LINUX_AMD64/$HASH_LINUX_AMD64/g" | \
   sed "s/HASH_LINUX_ARM64/$HASH_LINUX_ARM64/g" |
   sed "s/HASH_LINUX_ARM/$HASH_LINUX_ARM/g" > $tmpdir/kopia.rb

(cd $tmpdir && git add kopia.rb && git -c "user.name=Kopia Builder" -c "user.email=builder@kopia.io" commit -m "Brew formula update for kopia version $ver" && git push)
rm -rf "$tmpdir"
