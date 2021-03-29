# Spark Native SQL Engine Installation

For detailed testing scripts, please refer to [solution guide](https://github.com/Intel-bigdata/Solution_navigator/tree/master/nativesql)

## Install Googletest and Googlemock

``` shell
yum install gtest-devel
yum install gmock
```

## Build Native SQL Engine

``` shell
git clone -b ${version} https://github.com/oap-project/native-sql-engine.git
cd oap-native-sql
cd cpp/
mkdir build/
cd build/
cmake .. -DTESTS=ON
make -j
```

``` shell
cd ../../core/
mvn clean package -DskipTests
```

### Additonal Notes
[Notes for Installation Issues](./InstallationNotes.md)
