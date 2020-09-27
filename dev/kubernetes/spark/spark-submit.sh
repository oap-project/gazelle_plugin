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

WORK_DIR="$(dirname "$0")"

if [ ! -n "${SPARK_HOME}" ]; then
  echo "SPARK_HOME environment variable is not set."
  exit 1
fi

K8S_MASTER=localhost:8443
CONTAINER_IMAGE=spark-centos:1.0.0
SPARK_CONF=${WORK_DIR}/conf

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    -m|--master)
    shift 1 # past argument
    K8S_MASTER=$1
    shift 1 # past value
    ;;
    -i|--image)
    shift 1 # past argument
    CONTAINER_IMAGE=$1
    shift 1 # past value
    ;;
    -s|--spark_conf)
    shift 1 # past argument
    SPARK_CONF=$1
    shift 1 # past value
    ;;
    -h|--help)
    shift 1 # past argument
    echo "Usage: spark-submit.sh --master k8s-master:port --image image:tag --spark_conf spark-conf-dir --help other spark configuraitons"
    exit 1
    ;;
    *)    # completed this shell arugemnts procesing
    break
    ;;
esac
done

echo "Use Spark configuration at ${SPARK_CONF}"
export SPARK_CONF_DIR=${SPARK_CONF}

$SPARK_HOME/bin/spark-submit \
    --master k8s://https://$K8S_MASTER \
    --deploy-mode cluster \
    --conf spark.kubernetes.container.image=$CONTAINER_IMAGE \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    "$@"
