#!/bin/bash
mkdir source_code
cd source_code/
sudo -i
yum install -y numactl-devel libcurl-devel rpm-build zlib-devel sysstat pssh
exit

wget https://cmake.org/files/v3.12/cmake-3.12.3.tar.gz --no-check-certificate
tar zxvf cmake-3.12.3.tar.gz
cd cmake-3.12.3
./bootstrap --prefix=/usr/local --system-curl
make -j$(nproc)
sudo make install

cd ..
git config --global http.sslVerify false
git clone https://github.com/Intel-bigdata/arrow.git arrow
cd arrow
git checkout branch-0.17.0-oap-0.9

cd cpp
mkdir build
cd  build

cmake  -DCMAKE_INSTALL_PREFIX=/usr/ -DCMAKE_BUILD_TYPE=Release -DARROW_BUILD_TESTS=on -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED ..
make -j20
sudo make install
