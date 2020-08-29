#!/bin/bash

# set -e


git clone -b native-sql-engine-clean https://github.com/Intel-bigdata/spark.git 
cd spark

sed -i "s/<groupId>org.apache.spark<\/groupId>/<groupId>com.intel.spark<\/groupId>/g" `grep "<groupId>org.apache.spark</groupId>" ./ -rl`

sed -i '/<\/mailingLists>/a <distributionManagement>\n    <snapshotRepository>\n      <id>ossrh<\/id>\n      <url>https://oss.sonatype.org/content/repositories/snapshots<\/url>\n    <\/snapshotRepository>\n    <repository>\n      <id>ossrh<\/id>\n      <url>https://oss.sonatype.org/service/local/staging/deploy/maven2/<\/url>\n    <\/repository>\n  <\/distributionManagement>' pom.xml



./build/mvn  -Dcheckstyle.skip=true -DskipTests clean deploy



