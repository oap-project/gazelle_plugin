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
STATIC_ARROW(Default is Off): When BUILD_ARROW is ON, you can choose to build static or shared Arrow library
ARROW_ROOT(Default is /usr/local): When BUILD_ARROW is OFF, you can set the ARROW library path to link the existing library in your environment.
BUILD_PROTOBUF(Default is On): Build Protobuf from Source

``` shell
git clone -b ${version} https://github.com/oap-project/native-sql-engine.git
cd oap-native-sql
cd cpp/

//Edit compile.sh for mvn install
cmake .. -DTESTS=ON -DBUILD_ARROW=ON -DSTATIC_ARROW=OFF -DBUILD_PROTOBUF=ON

//For developers, building .so manually(Optional)
mkdir build/
cd build/
cmake .. -DTESTS=ON -DBUILD_ARROW=ON -DSTATIC_ARROW=OFF
make -j
```

``` shell
cd ../../core/
mvn clean package -DskipTests
```

### Additonal Notes
[Notes for Installation Issues](./InstallationNotes.md)
