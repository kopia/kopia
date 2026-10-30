#!/bin/sh
set -e
output_dir=$1
basefname=$2
binary_file=$3


temp_dir=$(mktemp -d)
staging_dir="$temp_dir/$basefname"
mkdir -p "$staging_dir"
cp $binary_file $staging_dir
cp LICENSE $staging_dir
cp README.md $staging_dir
tar -C $temp_dir -cvz $basefname > $output_dir/$basefname.tar.gz
rm -rf "$temp_dir"

#rm -rf $output_file.staging
#mkdir -vp $output_file.staging/$bn
# cp -v $output_file