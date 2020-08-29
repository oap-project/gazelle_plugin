#!/bin/bash

# set -e


git clone -b native-sql-engine-clean https://github.com/Intel-bigdata/spark.git 
cd spark

sed -i "s/<groupId>org.apache.spark<\/groupId>/<groupId>com.intel.spark<\/groupId>/g" `grep "<groupId>org.apache.spark</groupId>" ./ -rl`

sed -i "s/<version>3.1.0-SNAPSHOT<\/version>/<version>3.1.0-Beta<\/version>/g" `grep "<version>3.1.0-SNAPSHOT</version>" ./ -rl`

sed -i '/<parent>/, /<\/parent>/d'  pom.xml

sed -i '/<\/mailingLists>/a <distributionManagement>\n    <snapshotRepository>\n      <id>ossrh<\/id>\n      <url>https://oss.sonatype.org/content/repositories/snapshots<\/url>\n    <\/snapshotRepository>\n    <repository>\n      <id>ossrh<\/id>\n      <url>https://oss.sonatype.org/service/local/staging/deploy/maven2/<\/url>\n    <\/repository>\n  <\/distributionManagement>' pom.xml


sed -i '/<\/profiles>/i         <profile>\n            <id>deploy<\/id>\n            <build>\n                <plugins>\n                    <plugin>\n                        <groupId>org.apache.maven.plugins<\/groupId>\n                        <artifactId>maven-gpg-plugin<\/artifactId>\n                        <version>1.5<\/version>\n                        <executions>\n                            <execution>\n                                <id>sign-artifacts<\/id>\n                                <phase>verify<\/phase>\n                                <goals>\n                                    <goal>sign<\/goal>\n                                <\/goals>\n                            <\/execution>\n                        <\/executions>\n                    <\/plugin>\n                <\/plugins>\n            <\/build>\n        <\/profile>' pom.xml


./build/mvn  -Pdeploy  -Dcheckstyle.skip=true -DskipTests clean deploy




