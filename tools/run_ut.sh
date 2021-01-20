#!/bin/sh

# This script is used to run native sql unit test
# Usage: ./run_ut.sh
# Detailed test info is logged to oap-native-sql/tools/log-file.log

cd ../../
mvn test -pl oap-native-sql/core -am -DfailIfNoTests=false -Dmaven.test.failure.ignore=true &> oap-native-sql/tools/log-file.log

cd oap-native-sql/tools/
tests_total=0
module_tested=0
module_should_test=4
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
echo "Tests total: $tests_total, Succeed Total: $succeed_total"
 
if test $tests_total -eq $succeed_total -a $module_tested -eq $module_should_test
then
  echo "All unit tests succeed"
else
  echo "Unit tests failed, please check log-file.log for detailed info"
  exit 1
fi
