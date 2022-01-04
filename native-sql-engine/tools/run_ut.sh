#!/bin/bash

# This script is used to run native sql unit test
# SPARK_HOME is required, Usage: ./run_ut.sh
# Detailed test info is logged to oap-native-sql/tools/log-file.log

cd ../..
spark_home=$(eval echo ${SPARK_HOME})
if [ -z "${spark_home}" ]
then
  echo "SPARK_HOME is not set!"
  exit 1
else
  echo "SPARK_HOME is $spark_home"
fi
mvn clean test -P full-scala-compiler -Dbuild_arrow=OFF -Dbuild_protobuf=OFF -DfailIfNoTests=false -DargLine="-Dspark.test.home=$spark_home" -Dexec.skip=true -Dmaven.test.failure.ignore=true &>  native-sql-engine/tools/log-file.log
cd native-sql-engine/tools/

known_fails=111
tests_total=0
module_tested=0
module_should_test=7
while read -r line ; do
  num=$(echo "$line" | grep -o -E '[0-9]+')
  tests_total=$((tests_total+num))
done <<<"$(grep "Total number of tests run:" log-file.log)"

succeed_total=0
while read -r line ; do
  [[ $line =~ [^0-9]*([0-9]+)\, ]]
  num=${BASH_REMATCH[1]}
  succeed_total=$((succeed_total+num))
  let module_tested++
done <<<"$(grep "succeeded" log-file.log)"
failed_count=$((tests_total-succeed_total))
echo "Tests total: $tests_total, Succeed Total: $succeed_total, Known Fails: $known_fails, Actual Fails: $failed_count."

cat log-file.log | grep "\*** FAILED \***" | grep -v "TESTS FAILED ***" | grep -v "TEST FAILED ***" &> new_failed_list.log
comm -1 -3 <(sort failed_ut_list.log) <(sort new_failed_list.log) &> newly_failed_tests.log
comm -2 -3 <(sort failed_ut_list.log) <(sort new_failed_list.log) &> fixed_tests.log
if [ -s newly_failed_tests.log ]
then
    echo "Below are newly failed tests:"
    while read p; do
        echo "$p"
    done <newly_failed_tests.log
    mv log-file.log ../../perf_script/log/
    mv newly_failed_tests.log ../../perf_script/log/
    mv fixed_tests.log ../../perf_script/log/
    exit 1
else
    mv log-file.log ../../perf_script/log
    mv newly_failed_tests.log ../../perf_script/log/
    mv fixed_tests.log ../../perf_script/log/
    echo "No newly failed tests found!"
fi

if test $failed_count -le $known_fails -a $module_tested -eq $module_should_test
then
  echo "Unit tests succeeded!"
else
  echo "Unit tests failed, please check log-file.log for detailed info!"
  exit 1
fi
