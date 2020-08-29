#!/bin/bash

# set -e

git clone -b oap-master https://github.com/Intel-bigdata/arrow.git
cd arrow/java/

sed -i "s/<groupId>org.apache.arrow<\/groupId>/<groupId>com.intel.arrow<\/groupId>/g" `grep "<groupId>org.apache.arrow</groupId>" ./ -rl`

#change version
#sed -i "s/<version>0.17.0<\/version>/<version>0.17.0-rc1<\/version>/g" `grep "<version>0.17.0</version>" ./ -rl`

sed -i '/<parent>/, /<\/parent>/d'  pom.xml

sed -i '/<\/mailingLists>/a <distributionManagement>\n    <snapshotRepository>\n      <id>ossrh<\/id>\n      <url>https://oss.sonatype.org/content/repositories/snapshots<\/url>\n    <\/snapshotRepository>\n    <repository>\n      <id>ossrh<\/id>\n      <url>https://oss.sonatype.org/service/local/staging/deploy/maven2/<\/url>\n    <\/repository>\n  <\/distributionManagement>' pom.xml
sed -i '/<\/mailingLists>/a     <developers>\n    <developer>\n      <name>xiangxiang shen<\/name>\n      <email>xiangxiang.shen@intel.com<\/email>\n      <organization>intel bigdata<\/organization>\n      <organizationUrl>https://github.com/Intel-bigdata<\/organizationUrl>\n    <\/developer>\n  <\/developers>' pom.xml
sed -i '/<\/mailingLists>/a   <licenses>\n    <license>\n      <name>The Apache License, Version 2.0<\/name>\n      <url>http://www.apache.org/licenses/LICENSE-2.0.txt<\/url>\n    <\/license>\n  <\/licenses>' pom.xml


sed -i '/<\/profiles>/i         <profile>\n            <id>deploy<\/id>\n            <build>\n                <plugins>\n                                <plugin>\n      <groupId>org.apache.maven.plugins<\/groupId>\n      <artifactId>maven-source-plugin<\/artifactId>\n      <version>2.2.1<\/version>\n      <executions>\n        <execution>\n          <id>attach-sources<\/id>\n          <goals>\n            <goal>jar-no-fork<\/goal>\n          <\/goals>\n        <\/execution>\n      <\/executions>\n    <\/plugin>\n    <plugin>\n      <groupId>org.apache.maven.plugins<\/groupId>\n      <artifactId>maven-javadoc-plugin<\/artifactId>\n      <version>2.9.1<\/version>\n      <executions>\n        <execution>\n          <id>attach-javadocs<\/id>\n          <goals>\n            <goal>jar<\/goal>\n          <\/goals>\n        <\/execution>\n      <\/executions>\n    <\/plugin>\n   <plugin>\n                        <groupId>org.apache.maven.plugins<\/groupId>\n                        <artifactId>maven-gpg-plugin<\/artifactId>\n                        <version>1.5<\/version>\n                        <executions>\n                            <execution>\n                                <id>sign-artifacts<\/id>\n                                <phase>verify<\/phase>\n                                <goals>\n                                    <goal>sign<\/goal>\n                                <\/goals>\n                            <\/execution>\n                        <\/executions>\n                    <\/plugin>\n                <\/plugins>\n            <\/build>\n        <\/profile>' pom.xml

#select modules

#deploy
mvn  -Pdeploy  -Dcheckstyle.skip=true -DskipTests clean deploy



