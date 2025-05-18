#!/bin/sh
set -xe

if [ -z "$CI_TAG" ]; then
    echo "CI_TAG is not set. Looking for previous tag."
    start_commit=$(git describe --tags --abbrev=0 HEAD^)
    end_commit=HEAD
else
    echo "CI_TAG is set to $CI_TAG. Using it as the start commit."
    start_commit=$(git describe --tags --abbrev=0 $CI_TAG^)
    end_commit=$CI_TAG
fi

echo "Generating changelog from $start_commit to $end_commit"

toolsdir=$(realpath $(dirname $0))
config_file="$toolsdir/../.chglog/config-htmlui.yml"
output_file=$1

old_new_htmluibuild_hash=$(git diff $start_commit $end_commit -- go.mod | grep htmluibuild | cut -f 2 -d' ' | cut -f3 -d'-')
if [ -z "$old_new_htmluibuild_hash" ]; then
    echo "No changes found in htmluibuild between $start_commit and $end_commit"
    exit 1
fi


# extract the old and new htmluibuild hashes
old_htmluibuild_hash=$(echo $old_new_htmluibuild_hash | cut -f1 -d " ")
new_htmluibuild_hash=$(echo $old_new_htmluibuild_hash | cut -f2 -d " ")

echo "old_htmluibuild_hash: $old_htmluibuild_hash"
echo "new_htmluibuild_hash: $new_htmluibuild_hash"

# shallow clone of htmluibuild repo
htmluibuild_git_dir=/tmp/tmp-htmluibuild
if [ -d $htmluibuild_git_dir ]; then
    rm -rf $htmluibuild_git_dir
fi

git clone --filter=blob:none --no-checkout --single-branch --branch main https://github.com/kopia/htmluibuild $htmluibuild_git_dir

# extract htmlui hashes from the htmluibuild repo commit messages (all automated)
old_htmlui_hash=$(cd $htmluibuild_git_dir && git show $old_htmluibuild_hash | grep "HTMLUI update for" | cut -d / -f7)
new_htmlui_hash=$(cd $htmluibuild_git_dir && git show $new_htmluibuild_hash | grep "HTMLUI update for" | cut -d / -f7)

# shallow clone of htmlui repo
htmlui_git_dir=/tmp/tmp-htmlui
if [ -d $htmlui_git_dir ]; then
    rm -rf $htmlui_git_dir
fi

git clone --filter=blob:none --no-checkout --single-branch --branch main https://github.com/kopia/htmlui $htmlui_git_dir

(cd $htmlui_git_dir && 
    git config user.email "builder@kopia.io" &&
    git config user.name "Kopia Builder" &&
    git tag v0.1.0 $old_htmlui_hash -m "hash $old_htmlui_hash" && 
    git tag v0.2.0 $new_htmlui_hash -m "hash $new_htmlui_hash" && 
    $gitchglog --sort=semver --config=$config_file v0.2.0) >> $output_file

rm -rf $htmlui_git_dir
rm -rf $htmluibuild_git_dir