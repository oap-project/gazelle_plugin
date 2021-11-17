# Spark Configurations for Gazelle Plugin

There are many configuration could impact the Gazelle Plugin performance and can be fine tune in Spark.
You can add these configuration into spark-defaults.conf to enable or disable the setting.

| Parameters | Description | Recommend Setting |
| ---------- | ----------- | --------------- |
| spark.driver.extraClassPath | To add Arrow Data Source and Gazelle Plugin jar file in Spark Driver | /path/to/jar_file1:/path/to/jar_file2 |
| spark.executor.extraClassPath | To add Arrow Data Source and Gazelle Plugin jar file in Spark Executor | /path/to/jar_file1:/path/to/jar_file2 |
| spark.executorEnv.LIBARROW_DIR | To set up the location of Arrow library, by default it will search the loation of jar to be uncompressed | /path/to/arrow_library/ |
| spark.executorEnv.CC | To set up the location of gcc | /path/to/gcc/ |
| spark.executor.memory| To set up how much memory to be used for Spark Executor. | |
| spark.memory.offHeap.size| To set up how much memory to be used for Java OffHeap.<br /> Please notice Gazelle Plugin will leverage this setting to allocate memory space for native usage even offHeap is disabled. <br /> The value is based on your system and it is recommended to set it larger if you are facing Out of Memory issue in Gazelle Plugin | 30G |
| spark.sql.sources.useV1SourceList | Choose to use V1 source | avro |
| spark.sql.join.preferSortMergeJoin | To turn on/off preferSortMergeJoin in Spark. In gazelle we recomend to turn off this to get better performance | true |
| spark.plugins | To turn on Gazelle Plugin | com.intel.oap.GazellePlugin |
| spark.shuffle.manager | To turn on Gazelle Columnar Shuffle Plugin | org.apache.spark.shuffle.sort.ColumnarShuffleManager |
| spark.sql.shuffle.partitions | shuffle partition size, it's recomended to use the same number of your total cores | 200 |
| spark.oap.sql.columnar.batchscan | Enable or Disable Columnar Batchscan, default is true | true |
| spark.oap.sql.columnar.hashagg | Enable or Disable Columnar Hash Aggregate, default is true | true |
| spark.oap.sql.columnar.projfilter | Enable or Disable Columnar Project and Filter, default is true | true |
| spark.oap.sql.columnar.codegen.sort | Enable or Disable Columnar Sort, default is true | true |
| spark.oap.sql.columnar.window | Enable or Disable Columnar Window, default is true | true |
| spark.oap.sql.columnar.shuffledhashjoin | Enable or Disable ShffuledHashJoin, default is true | true |
| spark.oap.sql.columnar.sortmergejoin | Enable or Disable Columnar Sort Merge Join, default is true | true |
| spark.oap.sql.columnar.union | Enable or Disable Columnar Union, default is true | true |
| spark.oap.sql.columnar.expand | Enable or Disable Columnar Expand, default is true | true |
| spark.oap.sql.columnar.broadcastexchange | Enable or Disable Columnar Broadcast Exchange, default is true | true |
| spark.oap.sql.columnar.nanCheck | Enable or Disable Nan Check, default is true | true |
| spark.oap.sql.columnar.hashCompare | Enable or Disable Hash Compare in HashJoins or HashAgg, default is true | true |
| spark.oap.sql.columnar.broadcastJoin | Enable or Disable Columnar BradcastHashJoin, default is true | true |
| spark.oap.sql.columnar.sortmergejoin.lazyread | Enable or Disable lazy reading on Sort result. On disable, whole partition will be cached before doing SortMergeJoin | false |
| spark.oap.sql.columnar.wholestagecodegen | Enable or Disable Columnar WholeStageCodeGen, default is true | true |
| spark.oap.sql.columnar.preferColumnar | Enable or Disable Columnar Operators, default is false.<br /> This parameter could impact the performance in different case. In some cases, to set false can get some performance boost. | false |
| spark.oap.sql.columnar.joinOptimizationLevel | Fallback to row operators if there are several continous joins | 6 |
| spark.sql.execution.arrow.maxRecordsPerBatch | Set up the Max Records per Batch | 10000 |
| spark.sql.execution.sort.spillThreshold | Set up the Max sort in memory threshold in bytes, default is disabled | -1 |
| spark.oap.sql.columnar.wholestagecodegen.breakdownTime | Enable or Disable metrics in Columnar WholeStageCodeGen | false |
| spark.oap.sql.columnar.tmp_dir | Set up a folder to store the codegen files, default is disabled | "" |
| spark.oap.sql.columnar.shuffle.customizedCompression.codec | Set up the codec to be used for Columnar Shuffle, default is lz4| lz4 |
| spark.oap.sql.columnar.numaBinding | Set up NUMABinding, default is false| true |
| spark.oap.sql.columnar.coreRange | Set up the core range for NUMABinding, only works when numaBinding set to true. <br /> The setting is based on the number of cores in your system(lscpu | grep node[0-4]). Use 72 cores as an example. | 0-17,36-53 &#124;18-35,54-71 |

## Example thrift-server configuration
Here's one example of the thrift-server configuration
```
THRIFTSERVER_CONFIG="--name ${runname}
--num-executors 72
--driver-memory 20g
--executor-memory 6g
--executor-cores 6
--master yarn
--deploy-mode client
--conf spark.executor.memoryOverhead=384
--conf spark.executorEnv.CC=/home/sparkuser/miniconda3/envs/arrow-new/bin/gcc
--conf spark.plugins=com.intel.oap.GazellePlugin
--conf spark.executorEnv.LD_LIBRARY_PATH=/home/sparkuser/miniconda3/envs/arrow-new/lib/:/home/sparkuser/miniconda3/envs/arrow-new/lib64/
--conf spark.executorEnv.LIBARROW_DIR=/home/sparkuser/miniconda3/envs/arrow-new
--conf spark.driver.extraClassPath=${nativesql_jars}
--conf spark.executor.extraClassPath=${nativesql_jars}
--conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager
--conf spark.sql.join.preferSortMergeJoin=false
--conf spark.sql.inMemoryColumnarStorage.batchSize=${batchsize}
--conf spark.sql.execution.arrow.maxRecordsPerBatch=${batchsize}
--conf spark.sql.parquet.columnarReaderBatchSize=${batchsize}
--conf spark.sql.autoBroadcastJoinThreshold=10M
--conf spark.sql.broadcastTimeout=600
--conf spark.sql.crossJoin.enabled=true
--conf spark.driver.maxResultSize=20g
--hiveconf hive.server2.thrift.port=10001
--hiveconf hive.server2.thrift.bind.host=sr270
--conf spark.sql.codegen.wholeStage=true
--conf spark.sql.shuffle.partitions=432
--conf spark.memory.offHeap.enabled=true
--conf spark.memory.offHeap.size=15g
--conf spark.kryoserializer.buffer.max=128m
--conf spark.kryoserializer.buffer=32m
--conf spark.oap.sql.columnar.preferColumnar=false
--conf spark.oap.sql.columnar.sortmergejoin.lazyread=true
--conf spark.sql.execution.sort.spillThreshold=2147483648
--conf spark.executorEnv.LD_PRELOAD=/home/sparkuser/miniconda3/envs/arrow-new/lib/libjemalloc.so
--conf spark.executorEnv.MALLOC_CONF=background_thread:true,dirty_decay_ms:0,muzzy_decay_ms:0,narenas:2
--conf spark.executorEnv.MALLOC_ARENA_MAX=2
--conf spark.oap.sql.columnar.numaBinding=true
--conf spark.oap.sql.columnar.coreRange=0-35,72-107|36-71,108-143
--conf spark.oap.sql.columnar.joinOptimizationLevel=18
--conf spark.oap.sql.columnar.shuffle.customizedCompression.codec=lz4
--conf spark.yarn.appMasterEnv.LD_PRELOAD=/home/sparkuser/miniconda3/envs/arrow-new/lib/libjemalloc.so"
```

Below is an example for spark-default.conf, if you are using conda to install OAP project.


## Example spark-defaults.conf
```
spark.sql.sources.useV1SourceList avro
spark.sql.join.preferSortMergeJoin false
spark.plugins com.intel.oap.GazellePlugin
spark.shuffle.manager org.apache.spark.shuffle.sort.ColumnarShuffleManager

# note Gazelle Plugin depends on arrow data source
spark.driver.extraClassPath $HOME/miniconda2/envs/oapenv/oap_jars/spark-columnar-core-<version>-jar-with-dependencies.jar:$HOME/miniconda2/envs/oapenv/oap_jars/spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar
spark.executor.extraClassPath $HOME/miniconda2/envs/oapenv/oap_jars/spark-columnar-core-<version>-jar-with-dependencies.jar:$HOME/miniconda2/envs/oapenv/oap_jars/spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar

spark.executorEnv.LIBARROW_DIR      $HOME/miniconda2/envs/oapenv
spark.executorEnv.CC                $HOME/miniconda2/envs/oapenv/bin/gcc
######
```

Before you start spark, you must use below command to add some environment variables.

```
export CC=$HOME/miniconda2/envs/oapenv/bin/gcc
export LIBARROW_DIR=$HOME/miniconda2/envs/oapenv/
```
## Notes on driver
In gazelle spark driver is used to C++ code generation for different operators. This means driver takes more tasks than vanilla Spark, so it's better to consider allocate more resource to driver. By default, driver will compile C++ codes with best optimizations targeting for local CPU architecture:
```
-O3 -march=native
```
This could be override by a local environment variable before starting driver:
```
export CODEGEN_OPTION=" -O1 -mavx2 -fno-semantic-interposition "
```
