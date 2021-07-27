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

sh $SPARK_HOME/sbin/start-thriftserver.sh \
    --master k8s://$kubernetes_svc_address \
    --name spark-thrift-server \
    --conf spark.kubernetes.driver.pod.name=$HOSTNAME \
    --conf spark.driver.host=$spark_driver_host  \
    --conf spark.driver.port=$spark_driver_port \
    --conf spark.kubernetes.container.image=$spark_kubernetes_container_image \
    "$@"

tail -f $SPARK_HOME/conf/spark-defaults.conf.template
