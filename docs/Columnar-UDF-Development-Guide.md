## Columnar UDF Development Guide

Developer can implement columnar Hive UDF or scala UDF in Gazelle for performance benefits.

The original UDF still needs to be registered to Spark for two reasons: 1) Spark will create a kind of
expression for the registered UDF, which makes expression replacement possible. 2) expression fallback
still need work well for cases that the expression tree has some expression currently unsupported by Gazelle.

### Hive UDF

Suppose there is a UDF for handling string type input.

```
package com.intel.test;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MyHiveUDF extends UDF {

  public String evaluate(String b) {
    ...
  }
}
```

The jar contains this class should be put into Spark class path. Then, the UDF can be registered
to Spark at runtime as the below shows.

`spark.sql("CREATE TEMPORARY FUNCTION MyHiveUDFName AS 'com.intel.test.MyHiveUDF';")`

`MyHiveUDFName` is the unique name for UDF and it's case insensitive to Spark & Gazelle.

We need to implement a native version of `evaluate` function in `arrow/gandiva`. And in Gazelle
`ColumnarExpressionConverter.scala`, we need to add some logic to find the expression whose pretty
name is `MyHiveUDFName`, and replace it with the implemented columnar expression. The columnar
expression will call gandiva function for evaluating given input.

### Scala UDF

In Gazelle, the implementation for scala UDF is similar to Hive UDF. There is only few difference
in finding the matched expression for replacing it. See code details in `ColumnarExpressionConverter.scala`.

We still need to register the original scala UDF to Spark, e.g.,

`spark.udf.register("MyScalaUDFName", (s : String) => s)`

It is also required to implement a columnar expression and a gandiva function, similar to Hive UDF.

### Reference to Developer

[Support a UDF: URLDecoder](https://github.com/oap-project/gazelle_plugin/pull/925)