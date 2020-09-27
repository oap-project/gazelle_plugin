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

K8S_SVC_ADDRESS=https://kubernetes.default.svc.cluster.local:443
CONTAINER_IMAGE=spark-centos:1.0.0
SPARK_CONF=${WORK_DIR}/conf/
ACTION=shell

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    -s|--servce)
    shift 1 # past argument
    K8S_SVC_ADDRESS=$1
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
    echo "Usage: spark-[shell|sql|submit]-client.sh start|stop --service k8s-service-address --image image:tag --spark_conf spark-conf-dir --help "
    exit 1
    ;;
    *)    # action option
    ACTION=$1
    shift 1 # past argument
    ;;
esac
done

ESCAPTED_K8S_SVC_ADDRESS=$(printf '%s\n' "$K8S_SVC_ADDRESS" | sed -e 's/[\/&]/\\&/g')
ESCAPTED_CONTAINER_IMAGE=$(printf '%s\n' "$CONTAINER_IMAGE" | sed -e 's/[\/&]/\\&/g')

case "$ACTION" in
  start)
    shift 1
    echo "Using Spark configuraiton at ${SPARK_CONF}"
    # create spark configmap for Spark conf directory
    kubectl create configmap spark-client-conf --from-file=${SPARK_CONF}
    
    #kubectl apply -f ./spark-client-configmap.yaml
    cat ${WORK_DIR}/spark-client-configmap.yaml | \
    sed 's/\$K8S_SVC_ADDRESS'"/$ESCAPTED_K8S_SVC_ADDRESS/g" | \
    sed 's/\$CONTAINER_IMAGE'"/$ESCAPTED_CONTAINER_IMAGE/g" | \
    kubectl apply -f -
    
    # create headless service
    kubectl apply -f ${WORK_DIR}/spark-client-headless-service.yaml
    
    #kubectl apply -f ./spark-client.yaml
    cat ${WORK_DIR}/spark-client.yaml | \
    sed 's/\$CONTAINER_IMAGE'"/$ESCAPTED_CONTAINER_IMAGE/g" | \
    kubectl apply -f -
    ;;
  stop)
    shift 1
    kubectl delete pod spark-client
    kubectl delete svc spark-client-headless-service
    kubectl delete configmap spark-client-configmap
    kubectl delete configmap spark-client-conf
    ;;

  *)
    kubectl exec --stdin --tty spark-client -- /bin/bash
    ;;
esac
