#!/bin/bash
set -e
input=$1
signature=$2

# add signature to RPMs
if [ ${input: -4} == ".rpm" ]; then
  rpm --define "%_gpg_name Kopia Builder" --addsign $input
fi

if [ $input == "dist/checksums.txt" ]; then
    # before signing checksums.txt, regenerate it since we've just signed some RPMs.
    filenames=$(cut -f 2- -d " " dist/checksums.txt)
    (cd dist && sha256sum $filenames > checksums.txt)
    gpg --output dist/checksums.txt.sig --detach-sig dist/checksums.txt
fi
