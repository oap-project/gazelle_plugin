## Columnar UDF Development Guide

Developer can implement columnar Hive UDF or scala UDF in Gazelle for performance benefits.
The original UDF still needs to be registered to spark for two reasons: 1) spark
will create corresponding expression for the registered UDF. And then the expression
can be replaced by Gazelle with columnar expression. 2) expression fallback still need work well for some cases.

### Hive UDF

Suppose there is a UDF used to handle string type input.

```
package com.intel.test;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MyHiveUDF extends UDF {

  public String evaluate(String b) {
    ...
  }
}
```

The jar contains this class should be put into spark class path. Then, the UDF can be registered
to spark at runtime as the below shows.

`spark.sql("CREATE TEMPORARY FUNCTION MyHiveUDFName AS 'com.intel.test.MyHiveUDF';")`

`MyHiveUDFName` is the unique name for UDF and it's case insensitive to Spark & Gazelle.

In Gazelle, we need to implement a native version of `evaluate` function in arrow/gandiva.
In Gazelle `ColumnarExpressionConverter.scala`, we need to add some logic to find the
expression whose pretty name is `MyHiveUDFName`, and replace it with the implemented columnar
expression. The columnar expression will call the gandiva function for evaluating given input.

### Scala UDF

In Gazelle, the implementation for scala UDF is similar to Hive UDF. There is only few difference in finding
the matched expression for replacing it. See code details in `ColumnarExpressionConverter.scala`.

We still need to register the original scala UDF to Spark, e.g.,
`spark.udf.register("MyScalaUDFName", (s : String) => s)`

The implementation for columnar expression and gandiva function is also required.