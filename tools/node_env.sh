toolsdir=$(realpath $(dirname $0))
node_version=`grep NODE_VERSION $toolsdir/tools.mk | head -1 | cut -f 2 -d =`
echo tools dir: $toolsdir node_version: $node_version
PATH=$toolsdir/.tools/node-$node_version/bin:$PATH
