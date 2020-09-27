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

case "$1" in
  start)
    shift 1
    sh ${WORK_DIR}/spark-client.sh start "$@"
    ;;
  stop)
    shift 1
    sh ${WORK_DIR}/spark-client.sh stop "$@"
    ;;
  -h|--help)
    shift 1 # past argument
    sh ${WORK_DIR}/spark-client.sh --help "$@"
    ;;
  *)
    kubectl exec --stdin --tty spark-client -- /bin/bash ./spark-submit.sh "$@"
    ;;
esac
