# OAP Developer Guide

This document contains the instructions & scripts on installing necessary dependencies and building OAP. 
You can get more detailed information from OAP each module below.

* [SQL Index and Data Source Cache](https://github.com/oap-project/sql-ds-cache/blob/master/docs/Developer-Guide.md)
* [PMem Common](https://github.com/oap-project/pmem-common)
* [PMem Shuffle](https://github.com/oap-project/pmem-shuffle#5-install-dependencies-for-shuffle-remote-pmem-extension)
* [Remote Shuffle](https://github.com/oap-project/remote-shuffle)
* [OAP MLlib](https://github.com/oap-project/oap-mllib)
* [Arrow Data Source](https://github.com/oap-project/arrow-data-source)
* [Native SQL Engine](https://github.com/oap-project/native-sql-engine)

## Building OAP

### Prerequisites for Building

OAP is built with [Apache Maven](http://maven.apache.org/) and Oracle Java 8, and mainly required tools to install on your cluster are listed below.

- [Cmake](https://help.directadmin.com/item.php?id=494)
- [GCC > 7](https://gcc.gnu.org/wiki/InstallingGCC)
- [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2)
- [Vmemcache](https://github.com/pmem/vmemcache)
- [HPNL](https://github.com/Intel-bigdata/HPNL)
- [PMDK](https://github.com/pmem/pmdk)  
- [OneAPI](https://software.intel.com/content/www/us/en/develop/tools/oneapi.html)
- [Arrow](https://github.com/Intel-bigdata/arrow)

- **Requirements for Shuffle Remote PMem Extension**  
If enable Shuffle Remote PMem extension with RDMA, you can refer to [PMem Shuffle](https://github.com/oap-project/pmem-shuffle) to configure and validate RDMA in advance.

We provide scripts below to help automatically install dependencies above **except RDMA**, need change to **root** account, run:

```
# git clone -b <tag-version> https://github.com/Intel-bigdata/OAP.git
# cd OAP
# sh $OAP_HOME/dev/install-compile-time-dependencies.sh
```

Run the following command to learn more.

```
# sh $OAP_HOME/dev/scripts/prepare_oap_env.sh --help
```

Run the following command to automatically install specific dependency such as Maven.

```
# sh $OAP_HOME/dev/scripts/prepare_oap_env.sh --prepare_maven
```


### Building

To build OAP package, run command below then you can find a tarball named `oap-$VERSION-bin-spark-$VERSION.tar.gz` under directory `$OAP_HOME/dev/release-package `.
```
$ sh $OAP_HOME/dev/compile-oap.sh
```

Building Specified OAP Module, such as `oap-cache`, run:
```
$ sh $OAP_HOME/dev/compile-oap.sh --oap-cache
```


### Running OAP Unit Tests

Setup building environment manually for intel MLlib, and if your default GCC version is before 7.0 also need export `CC` & `CXX` before using `mvn`, run

```
$ export CXX=$OAP_HOME/dev/thirdparty/gcc7/bin/g++
$ export CC=$OAP_HOME/dev/thirdparty/gcc7/bin/gcc
$ export ONEAPI_ROOT=/opt/intel/inteloneapi
$ source /opt/intel/inteloneapi/daal/2021.1-beta07/env/vars.sh
$ source /opt/intel/inteloneapi/tbb/2021.1-beta07/env/vars.sh
$ source /tmp/oneCCL/build/_install/env/setvars.sh
```

Run all the tests:

```
$ mvn clean test
```

Run Specified OAP Module Unit Test, such as `oap-cache`:

```
$ mvn clean -pl com.intel.oap:oap-cache -am test

```

### Building SQL Index and Data Source Cache with PMem

#### Prerequisites for building with PMem support

When using SQL Index and Data Source Cache with PMem, finish steps of [Prerequisites for building](#prerequisites-for-building) to ensure needed dependencies have been installed.

#### Building package

You can build OAP with PMem support with command below:

```
$ sh $OAP_HOME/dev/compile-oap.sh
```
Or run:

```
$ mvn clean -q -Ppersistent-memory -Pvmemcache -DskipTests package
```
