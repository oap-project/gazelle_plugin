# Performance tuning for Gazelle Plugin

It is complicated to tune for Spark workloads as each varies a lot. Here are several general tuning options on the most popular TPCH/TPC-DS benchmarking.

## Data Generating
On non-partiton tables, it's better to set the number of files to times of total HT cores in your cluster, e.g., there are 384 cores in your cluster, we'd better to set the file numbers to 2*384 or 3*384 to ensure. In this way Spark would only issue one task for each file(together with spark.sql.files.maxPartitionBytes option).

Note the spark.sql.files.maxRecordsPerFile will also split the files in to smaller ones. We may just disable this feature to -1.

## Shuffle Partitions
As Gazelle currently only supports hash based shuffle, it's recommended to use 1 or 2 times shuffle partitions of the total HT cores in the cluster. e.g., there are 384 cores in the cluster, it's better to use spark.sql.shuffle.partitions = 384 or 768. It this way it's most efficient for Gazelle. 

## On-heap/Off-heap Memory Size
Unlike Spark, most of the memory usage in Gazelle would be off-heap based. So a big off-heap is recomended. There are still some small objects in On-heap thus a proper sized on-heap is also required. We are recommending below configurations for memory related settings. 
```
--executor-cores 6
--executor-memory 6g // on-heap memory: 1G per core
--conf spark.executor.memoryOverhead=384 // not used, 384M should be enough
--conf spark.memory.offHeap.enabled=true // enable off-heap thus Spark can control the memory
--conf spark.memory.offHeap.size=15g // a big off-heap is required
```  
