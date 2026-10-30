#!/bin/sh
set -e

# install tools
sudo apt install -y fio python3-pip
sudo pip3 install psrecord

# format /mnt/data partition
sudo mkfs.ext4 -F /dev/nvme0n1
sudo mkdir /mnt/data
sudo mount /dev/nvme0n1 /mnt/data
sudo chown $USER /mnt/data
