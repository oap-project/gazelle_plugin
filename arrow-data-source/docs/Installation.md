# Spark Native SQL Engine Installation

For detailed testing scripts, please refer to [solution guide](https://github.com/Intel-bigdata/Solution_navigator/tree/master/nativesql)

## Install Googletest and Googlemock

``` shell
yum install gtest-devel
yum install gmock
```

## Build Native SQL Engine

cmake parameters:
BUILD_ARROW(Default is On): Build Arrow from Source
STATIC_ARROW(Default is Off): When BUILD_ARROW is ON, you can choose to build static or shared Arrow library, please notice current only support to build SHARED ARROW.
ARROW_ROOT(Default is /usr/local): When BUILD_ARROW is OFF, you can set the ARROW library path to link the existing library in your environment.
BUILD_PROTOBUF(Default is On): Build Protobuf from Source

``` shell
git clone -b ${version} https://github.com/oap-project/native-sql-engine.git
cd native-sql-engine
mvn clean package -am -DskipTests -Dcpp_tests=OFF -Dbuild_arrow=ON -Dstatic_arrow=OFF -Darrow_root=/usr/local -Dbuild_protobuf=ON
```

### Additonal Notes
[Notes for Installation Issues](./InstallationNotes.md)
