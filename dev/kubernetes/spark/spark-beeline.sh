#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ ! -n "${SPARK_HOME}" ]; then
  echo "SPARK_HOME environment variable is not set."
  exit 1
fi

K8S_MASTER_HOST=localhost

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    -m|--master)
    shift 1 # past argument
    K8S_MASTER_HOST=$1
    shift 1 # past value
    ;;
    -h|--help)
    shift 1 # past argument
    echo "Usage: spark-beline.sh --master k8s-master-host-ip "
    exit 1
    ;;
    *)    # unknown option
    echo "Unkown argement: $1. Use --help for the details."
    exit 1
    ;;
esac
done

$SPARK_HOME/bin/beeline -u jdbc:hive2://$K8S_MASTER_HOST:30000
