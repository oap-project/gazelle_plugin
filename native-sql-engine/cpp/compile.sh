#!/usr/bin/env bash

set -eu

TESTS=${1:-OFF}
BUILD_ARROW=${2:-ON}
STATIC_ARROW=${3:-OFF}
BUILD_PROTOBUF=${4:-ON}
ARROW_ROOT=${5:-/usr/local}
ARROW_BFS_INSTALL_DIR=${6}

echo "CMAKE Arguments:"
echo "TESTS=${TESTS}"
echo "BUILD_ARROW=${BUILD_ARROW}"
echo "STATIC_ARROW=${STATIC_ARROW}"
echo "BUILD_PROTOBUF=${BUILD_PROTOBUF}"
echo "ARROW_ROOT=${ARROW_ROOT}"
echo "ARROW_BUILD_FROM_SOURCE_INSTALL_DIR=${ARROW_BFS_INSTALL_DIR}"

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR

cd ${CURRENT_DIR}
#if [ -d build ]; then
#    rm -r build
#fi
#mkdir build
cd build
cmake .. -DTESTS=${TESTS} -DBUILD_ARROW=${BUILD_ARROW} -DSTATIC_ARROW=${STATIC_ARROW} -DBUILD_PROTOBUF=${BUILD_PROTOBUF} -DARROW_ROOT=${ARROW_ROOT} -DARROW_BFS_INSTALL_DIR=${ARROW_BFS_INSTALL_DIR}
make -j2

set +eu

