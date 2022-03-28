# Steps to create a Developer environment and compile Gazelle

```
conda env create -f gazelle_dev.yml
conda activate gazelle_dev
git clone https://github.com/oap-project/gazelle_plugin.git
cd gazelle_plugin/
mvn clean deploy -Dbuild_arrow=ON -Dbuild_protobuf=ON -Dbuild_jemalloc=ON -Dcpp_tests=OFF -Dcheckstyle.skip -Pfull-scala-compiler -DskipTests
```
