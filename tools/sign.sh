#!/usr/bin/env bash
set -e

for f in dist/*rpm; do
  # add signature to RPMs
  rpm --define "%_gpg_name Kopia Builder" --addsign $f
done

# before signing checksums.txt, regenerate it since we've just signed some RPMs.
filenames=$(cut -f 2- -d " " dist/checksums.txt)
(cd dist && sha256sum $filenames > checksums.txt)
gpg --output dist/checksums.txt.sig --detach-sig dist/checksums.txt
