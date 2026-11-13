#!/usr/bin/env bash
VERSION=$1

# stable,testing,unstable
CHANNEL=$2

# total size of source data to backup (e.g. 10Gi)
TOTAL_SIZE=$3

# we always setup files the same way, the only thing that varies is the number of files

fio_opts="--size=$TOTAL_SIZE --bs=256k --iodepth=32 --dedupe_percentage=40 --buffer_compress_percentage=60 --numjobs=1 --rw=write --name=/mnt/data/source/kopia-test"

set -e

echo Running performance test against version $VERSION from channel $CHANNEL with total data size $TOTAL_SIZE

# Install Kopia from APT repository...
curl -s https://kopia.io/signing-key | sudo apt-key add -
echo "deb http://packages.kopia.io/apt/ $CHANNEL main" | sudo tee /etc/apt/sources.list.d/kopia.list
sudo apt update
sudo apt install -y --allow-downgrades kopia=$VERSION

sudo chown -R $USER /mnt/data

for scenario in 1000-compressed 100-compressed 10-compressed 1000-uncompressed 100-uncompressed 10-uncompressed; do
    echo Cleaning up directories...
    rm -rfv /mnt/data/{repo,cache,source}
    mkdir -p /mnt/data/{repo,cache,source}

    # create 100 x 2 GB files
    echo Preparing files...

    export KOPIA_PASSWORD=super-secure
    kopia repo create filesystem --path=/mnt/data/repo --cache-directory=/mnt/data/cache

    case $scenario in
        1000-compressed)
            fio --nrfiles=1000 $fio_opts
            kopia policy set --global --compression=s2-default
            ;;
        100-compressed)
            fio --nrfiles=100 $fio_opts
            kopia policy set --global --compression=s2-default
            ;;
        10-compressed)
            fio --nrfiles=10 $fio_opts
            kopia policy set --global --compression=s2-default
            ;;
        1000-uncompressed)
            fio --nrfiles=1000 $fio_opts
            ;;
        100-uncompressed)
            fio --nrfiles=100 $fio_opts
            ;;
        10-uncompressed)
            fio --nrfiles=10 $fio_opts
            ;;
            *)
            echo Unhandled scenario $scenario
            exit 1
            ;;
    esac

    psrecord --interval 1 --include-children --log psrecord-$VERSION-initial-$scenario.log "kopia snap create /mnt/data/source"

    # clear cache
    rm -rfv /mnt/data/cache}
    mkdir -p /mnt/data/cache}

    # reconnect to repository
    kopia repo connect filesystem --path=/mnt/data/repo --cache-directory=/mnt/data/cache
    psrecord --interval 1 --include-children --log psrecord-$VERSION-second-$scenario.log "kopia snap create /mnt/data/source"

    # dump repo size
    du -bs /mnt/data/repo/ > repo-size-$VERSION-$scenario.log
done
