# OAP Developer Guide

This document contains the instructions & scripts on installing necessary dependencies and building OAP modules. 
You can get more detailed information from OAP each module below.


* [OAP MLlib](https://github.com/oap-project/oap-mllib/tree/v1.4.0)
* [Gazelle Plugin](https://github.com/oap-project/gazelle_plugin/tree/v1.4.0)

## Building OAP

### Prerequisites

We provide scripts to help automatically install dependencies required, please change to **root** user and run:

```
# git clone -b <tag-version> https://github.com/oap-project/oap-tools.git
# cd oap-tools
# sh dev/install-compile-time-dependencies.sh
```
*Note*: oap-tools tag version `v1.4.0` corresponds to  all OAP modules' tag version `v1.4.0`.

Then the dependencies below will be installed:

* [Cmake](https://cmake.org/install/)
* [GCC > 9](https://gcc.gnu.org/wiki/InstallingGCC)
* [OneAPI](https://software.intel.com/content/www/us/en/develop/tools/oneapi.html)
* [Arrow](https://github.com/oap-project/arrow/tree/v4.0.0-oap-1.4.0)
* [LLVM](https://llvm.org/) 


### Building

#### Building OAP 

OAP is built with [Apache Maven](http://maven.apache.org/) and Oracle Java 8.

To build OAP package, run command below then you can find a tarball named `oap-$VERSION-*.tar.gz` under directory `$OAP_TOOLS_HOME/dev/release-package `, which contains all OAP module jars.
Change to `root` user, run

```
# cd oap-tools
# sh dev/compile-oap.sh
```

#### Building OAP specific module 

If you just want to build a specific OAP Module, such as `gazelle_plugin`, change to `root` user, then run:

```
# cd oap-tools
# sh dev/compile-oap.sh --component=gazelle_plugin
```
