# Gazelle Plugin Installation

For detailed testing scripts, please refer to [solution guide](https://github.com/Intel-bigdata/Solution_navigator/tree/master/nativesql)

## Install Googletest and Googlemock

``` shell
yum install gtest-devel
yum install gmock
```

## Build Gazelle Plugin

``` shell
git clone -b ${version} https://github.com/oap-project/gazelle_plugin.git
cd gazelle_plugin
mvn clean package -Pspark-3.1 -DskipTests -Dcpp_tests=OFF -Dbuild_arrow=ON -Dcheckstyle.skip
```
Please note two Spark profiles (`spark-3.1`, `spark-3.2`) are provided to build packages with different versions of Spark dependencies.
Currently, a few unit tests are not compatible with spark 3.2. So if profile `spark-3.2` is used, `-Dmaven.test.skip` should be added to skip compiling unit tests.
```
mvn clean package -Pspark-3.2 -Dmaven.test.skip -Dcpp_tests=OFF -Dbuild_arrow=ON -Dcheckstyle.skip
```

Based on the different environment, there are some parameters can be set via -D with mvn.

| Parameters | Description | Default Value |
| ---------- | ----------- | ------------- |
| cpp_tests  | Enable or Disable CPP Tests | False |
| build_arrow | Build Arrow from Source | True |
| arrow_root | When build_arrow set to False, arrow_root will be enabled to find the location of your existing arrow library. | /usr/local |
| build_protobuf | Build Protobuf from Source. If set to False, default library path will be used to find protobuf library. | True |

When build_arrow set to True, the build_arrow.sh will be launched and compile a custom arrow library from [OAP Arrow](https://github.com/oap-project/arrow/tree/arrow-4.0.0-oap-1.3.1)
If you wish to change any parameters from Arrow, you can change it from the `build_arrow.sh` script under `native-sql-engine/arrow-data-source/script/`.

### Additional Notes
[Notes for Installation Issues](./InstallationNotes.md)
