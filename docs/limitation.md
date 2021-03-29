# Limitations for Native SQL Engine

## Spark compability
Native SQL engine currenlty works with Spark 3.0.0 only. There are still some trouble with latest Shuffle/AQE API from Spark 3.0.1, 3.0.2 or 3.1.x.

## Operator limitations
### Columnar Projection with Filter
We used 16 bit selection vector for filter so the max batch size need to be < 65536

### Columnar Sort
To reduce the peak memory usage, we used smaller data structure(uin16_t). This limits 
- the max batch size to be < 65536
- the number of batches in one partiton to be < 65536


