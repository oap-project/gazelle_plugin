
## Hive UDF Test Guide

Currently, Gazelle supports a UDF called URLDecoder whose original implementation is under `src/`.
To test a UDF, we should compile the original hive UDF code into a jar and put it into spark classpath.

Take URLDecoder as example.

1. Compile original Hive UDF code with `hive-exec` dependency provided.

2. Deploy the jar and configure it into Spark classpath. We can deploy it with Gazelle jar and make
   the below configuration to include them in spark classpath.

   ```
   --conf spark.driver.extraClassPath="YOUR_JAR_PATH"
   --conf spark.executor.extraClassPath="YOUR_JAR_PATH"
   ```

3. Register the UDF to Spark.

   ```
   spark.sql("CREATE TEMPORARY FUNCTION URLDecoder AS 'com.intel.oap.URLDecoderNew';")
   ```

4. Run SQL with UrlDecoder used.

   ```
   spark.sql("SELECT UrlDecoder(<YOUR_COL>) from <YOUR_TABLE>;")
   ```