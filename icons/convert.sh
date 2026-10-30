#!/bin/bash
# create various resolutions from source 1024x1024 PNG

set -e

make_icns() {
    source=$1
    output=$2

    mkdir MyIcon.iconset
    sips -z 16 16     $source --out MyIcon.iconset/icon_16x16.png
    sips -z 32 32     $source --out MyIcon.iconset/icon_16x16@2x.png
    sips -z 32 32     $source --out MyIcon.iconset/icon_32x32.png
    sips -z 64 64     $source --out MyIcon.iconset/icon_32x32@2x.png
    sips -z 128 128   $source --out MyIcon.iconset/icon_128x128.png
    sips -z 256 256   $source --out MyIcon.iconset/icon_128x128@2x.png
    sips -z 256 256   $source --out MyIcon.iconset/icon_256x256.png
    sips -z 512 512   $source --out MyIcon.iconset/icon_256x256@2x.png
    # sips -z 512 512   $source --out MyIcon.iconset/icon_512x512.png
    # cp $source MyIcon.iconset/icon_512x512@2x.png
    iconutil -c icns MyIcon.iconset
    rm -R MyIcon.iconset
    mv MyIcon.icns $output
}

make_ico() {
    source=$1
    output=$2
    convert $source -define icon:auto-resize="256,128,96,64,48,32,16" $output
}

# macOS icon
make_icns kopia-app-dark-1024.png ../app/assets/icon.icns

# Windows icon
make_ico kopia-app-win-1024.png ../app/assets/icon.ico

# macOS tray icon
sips -z 20 28 ../app/resources/mac/icons/kopiaTrayTemplate@2x.png --out ../app/resources/mac/icons/kopiaTrayTemplate.png

# Windows tray icon
convert kopia-outline-win-tray.png -define icon:auto-resize="32" ../app/resources/win/icons/kopia-tray.ico

# site
cp kopia-white.svg ../site/assets/icons/logo.svg
