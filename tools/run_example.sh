#!/bin/bash

#/home/spark-sql/collect_sar.sh
eventlogdir="spark_logs"

echo "Stoping Thrift Server ..."
./run_spark_thrift_server.sh stop
echo "Done"
sleep 20
echo "Cleaning Cache ..."
./clean_cache.sh
echo "Done"
sleep 1
echo "Starting Thrift Server ..."
./run_spark_thrift_server.sh start
echo "Done"
sleep 40
echo "Start Resource Monitoring ..."
appid=`yarn application -list 2>&1 | tail -n 1 | awk -F"\t" '{print $1}'`
echo `date +'%H %Y_%m_%d'` $appid ${1} >> log/runs.txt
rm -f log/memory*.csv
python3 ./monitor.py start $appid
echo "Running TPCH Query"
./run_tpch.py 2>&1 >> tpch_query.log | tee -a tpch_query.txt
echo "Done"
sleep 1
echo "Stop Thrift Server"
./run_spark_thrift_server.sh stop
sleep 10
python3 ./monitor.py stop $appid "spark_logs"
echo '<font style="font-family: Courier New"">' > log/link.html
echo 'history event: <a href="http://10.1.0.24:18080/history/'$appid'/jobs/">http://10.1.0.24:18080/history/'$appid'/jobs/</a><br>' >> log/link.html
echo 'history on sr124: <a href="http://10.1.0.24:18080/history/'$appid'/jobs/">http://10.1.0.24:18080/history/'$appid'/jobs/</a><br>' >> log/link.html
echo 'notebook on sr124: <a href="http://10.1.0.24:8888/notebooks/jenkins/tpch_'`date +'%Y_%m_%d'`'_'$appid'.ipynb">http://10.1.0.24:8888/notebooks/jenkins/tpch_'`date +'%Y_%m_%d'`'_'$appid'.ipynb</a><br>' >> log/link.html
echo 'notebook html on sr124: <a href="http://10.1.0.24:8888/view/jenkins/html/tpch_'`date +'%Y_%m_%d'`'_'$appid'.html">http://10.1.0.24:8888/notebooks/jenkins/html/tpch_'`date +'%Y_%m_%d'`'_'$appid'.html</a><br>' >> log/link.html
echo 'traceview on sr124: <a href="http://10.1.0.24:1088/tracing_examples/trace_viewer.html#/tracing/test_data/'$appid'.json">http://10.1.0.24:1088/tracing_examples/trace_viewer.html#/tracing/test_data/'$appid'.json</a><br>' >> log/link.html

echo "</font><hr/>" >> log/link.html

echo "All Jobs Are Done."
#/home/spark-sql/stop_sar.sh
