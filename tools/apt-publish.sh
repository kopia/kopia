#!/bin/bash
set -e
GS_PREFIX=gs://packages.kopia.io/apt
GPG_KEY_ID=A3B5843ED70529C23162E3687713E6D88ED70D9D
PKGDIR=$1

if [ -z "$PKGDIR" ]; then
  echo usage $0: /path/to/dist
  exit 1
fi

if [ ! -d "$PKGDIR" ]; then
  echo $PKGDIR must be a directory containing '*.deb' files
  exit 1
fi

distributions="stable testing unstable"
architectures="amd64 arm64 armhf"

WORK_DIR=/tmp/apt-publish
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

echo Downloading Package lists...

deb_files=$(find $1 -name '*.deb')

# download 'Packages' files from GCS for all supported distributions and architectures
for d in $distributions; do
  for a in $architectures; do
    mkdir -pv $WORK_DIR/dists/$d/main/binary-$a
    touch $WORK_DIR/dists/$d/main/binary-$a/Packages
    gsutil cp -av $GS_PREFIX/dists/$d/main/binary-$a/Packages $WORK_DIR/dists/$d/main/binary-$a/Packages
  done
done

# sort all files into appropriate binary directories
for f in $deb_files; do
  if [[ "$f" =~ _([^_]*)_(linux_)?([^_]+).deb$ ]]; then
    ver=${BASH_REMATCH[1]#v}
    arch=${BASH_REMATCH[3]}
    dists=""

    if [[ $ver =~ "next" ]]; then
      # ignore -next versions which are from goreleaser snapshots
      continue
    fi

    # x.y.z
    if [[ $ver =~ [0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      dists="stable testing"
    fi

    # x.y.z-prerelease
    if [[ $ver =~ [0-9]+\.[0-9]+\.[0-9]+\-.*$ ]]; then
      dists="testing"
    fi

    # yyyymmdd.0.hhmmss starts with 20
    if [[ $ver =~ 20[0-9]+\.[0-9]+\.[0-9]+ ]]; then
      dists="unstable"
    fi

    echo "$f $arch $dists"

    bn=$(basename $f)
    for d in $dists; do
      packages_dir=$WORK_DIR/dists/$d/main/binary-$arch
      if grep $bn\$ $packages_dir/Packages > /dev/null; then
        echo $bn already in $packages_dir/Packages
      else
        cp -av $f $packages_dir
      fi
    done
  fi
done

# append to 'Packages' file for all files placed in the work directory
for d in $distributions; do
  for a in $architectures; do
    mkdir -pv $WORK_DIR/dists/$d/main/binary-$a
    (cd $WORK_DIR && dpkg-scanpackages --multiversion -a $a dists/$d/main/binary-$a >> dists/$d/main/binary-$a/Packages)
    gzip -k $WORK_DIR/dists/$d/main/binary-$a/Packages
  done
done

# generate Release/InRelease/Release.gpg files for all distributions
for d in $distributions; do
  docker run -it --rm -v $WORK_DIR/dists/$d:/root marshallofsound/apt-ftparchive release \
    -o APT::FTPArchive::Release::Architectures="$architectures" \
    -o APT::FTPArchive::Release::Codename="$d" \
    -o APT::FTPArchive::Release::Suite="$d" \
    . > $WORK_DIR/dists/$d/Release

  gpg --default-key $GPG_KEY_ID -abs -o - $WORK_DIR/dists/$d/Release > $WORK_DIR/dists/$d/Release.gpg
  gpg --default-key $GPG_KEY_ID --clearsign -o - $WORK_DIR/dists/$d/Release > $WORK_DIR/dists/$d/InRelease
done

# sync back to GCS
echo Synchronizing...
gsutil -m rsync -r $WORK_DIR/ $GS_PREFIX/

# reapply caching parameters
echo Setting caching parameters...
gsutil -m setmeta -h "Cache-Control:no-cache, max-age=0" $GS_PREFIX/dists/{stable,testing,unstable}/{Release,Release.gpg,InRelease}
gsutil -m setmeta -h "Cache-Control:no-cache, max-age=0" $GS_PREFIX/dists/{stable,testing,unstable}/main/binary-{amd64,arm64,armhf}/Packages{,.gz}

echo Done.