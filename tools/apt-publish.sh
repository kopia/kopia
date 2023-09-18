#!/bin/bash
set -e
GS_PREFIX=gs://$PACKAGES_HOST/apt
GPG_KEY_ID=7FB99DFD47809F0D5339D7D92273699AFD56A556
PKGDIR=$1
RETAIN_UNSTABLE_DEB_COUNT=3

if [ -z "$PACKAGES_HOST" ]; then
  echo Not publishing APT package because PACKAGES_HOST is not set.
  exit 0
fi

if [ -z "$PKGDIR" ]; then
  echo usage $0: /path/to/dist
  exit 1
fi

if [ ! -d "$PKGDIR" ]; then
  echo $PKGDIR must be a directory containing '*.deb' files
  exit 1
fi

delete_old_deb() {
  ls -tp1 $1/*.deb | tail -n +$RETAIN_UNSTABLE_DEB_COUNT | xargs -I {} rm -v -- {}
}

distributions="unstable"

if [ "$CI_TAG" != "" ]; then
  distributions="stable testing"
fi

# note we don't produce packages for i386 but lack of it confuses some amd64 clients.
architectures="amd64 arm64 armhf i386"

WORK_DIR=/tmp/apt-publish
# rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

echo Downloading packages...

deb_files=$(find $1 -name '*.deb')

# download files files from GCS for all supported distributions and architectures
for d in $distributions; do
  mkdir -pv $WORK_DIR/dists/$d
  gsutil -m rsync -r -d $GS_PREFIX/dists/$d $WORK_DIR/dists/$d
  for a in $architectures; do
    delete_old_deb $WORK_DIR/dists/$d/main/binary-$a || echo Unable to delete old deb
  done
done

echo Sorting...

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


    if [[ "$architectures" != *"$arch"* ]]; then
      echo "ignoring unsupported architecture: $f arch: $arch dists: $dists"
      continue
    fi

    echo "$f $arch $dists"

    bn=$(basename $f)
    for d in $dists; do
      packages_dir=$WORK_DIR/dists/$d/main/binary-$arch
      if grep $bn\$ $packages_dir/Packages > /dev/null; then
        echo $bn already in $packages_dir/Packages
      else
        mkdir -p $packages_dir
        cp -av $f $packages_dir
      fi
    done
  fi
done

echo Generating Packages...

# append to 'Packages' file for all files placed in the work directory
for d in $distributions; do
  for a in $architectures; do
    mkdir -pv $WORK_DIR/dists/$d/main/binary-$a
    (cd $WORK_DIR && dpkg-scanpackages --multiversion -a $a dists/$d/main/binary-$a > dists/$d/main/binary-$a/Packages)
    gzip -kf $WORK_DIR/dists/$d/main/binary-$a/Packages
  done
done

echo Generating Release files

# generate Release/InRelease/Release.gpg files for all distributions
for d in $distributions; do
  echo Generating $WORK_DIR/dists/$d/Release...
  docker run -i --rm -v $WORK_DIR/dists/$d:/root marshallofsound/apt-ftparchive release \
    -o APT::FTPArchive::Release::Architectures="$architectures" \
    -o APT::FTPArchive::Release::Codename="$d" \
    -o APT::FTPArchive::Release::Suite="$d" \
    . > $WORK_DIR/dists/$d/Release

  echo Signing $WORK_DIR/dists/$d/Release.gpg
  gpg --default-key $GPG_KEY_ID -abs -o - $WORK_DIR/dists/$d/Release > $WORK_DIR/dists/$d/Release.gpg
  gpg --default-key $GPG_KEY_ID --clearsign -o - $WORK_DIR/dists/$d/Release > $WORK_DIR/dists/$d/InRelease
done

for d in $distributions; do
  # sync back to GCS
  echo Synchronizing...
  gsutil -m rsync -r -d $WORK_DIR/dists/$d $GS_PREFIX/dists/$d

  # reapply caching parameters
  echo Setting caching parameters...
  gsutil -m setmeta -h "Cache-Control:no-cache, max-age=0" $GS_PREFIX/dists/$d/{Release,Release.gpg,InRelease}
  gsutil -m setmeta -h "Cache-Control:no-cache, max-age=0" $GS_PREFIX/dists/$d/main/binary-{amd64,arm64,armhf}/Packages{,.gz}
done

echo Done.
