#!/usr/bin/env bash

echo "Installing oneAPI components ..."
cd /tmp
tee > /tmp/oneAPI.repo << EOF
[oneAPI]
name=Intel(R) oneAPI repository
baseurl=https://yum.repos.intel.com/oneapi
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://yum.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2023.PUB
EOF
sudo mv /tmp/oneAPI.repo /etc/yum.repos.d
sudo yum install intel-oneapi-daal-devel intel-oneapi-tbb-devel

echo "Building oneCCL ..."
cd /tmp
git clone https://github.com/oneapi-src/oneCCL
cd oneCCL && mkdir build && cd build
cmake ..
make -j 2 install
