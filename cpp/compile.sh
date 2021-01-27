#!/usr/bin/env bash

set -eu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR

cd ${CURRENT_DIR}
if [ -d build ]; then
    rm -r build
fi
mkdir build
cd build
cmake .. -DTESTS=ON -DBUILD_ARROW=ON -DSTATIC_ARROW=OFF -DBUILD_PROTOBUF=ON
make -j

set +eu

