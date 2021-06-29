/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.nativesql

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession

class NativeSQLConvertedSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("BHJ") {
    Seq(("one", 1), ("two", 2), ("three", 3), ("one", 3))
      .toDF("k", "v").createOrReplaceTempView("t1")
    Seq(("one", 1), ("two", 22), ("one", 5), ("one", 7), ("two", 5))
      .toDF("k", "v").createOrReplaceTempView("t2")

    val df = sql("SELECT t1.* FROM t1, t2 where t1.k = t2.k " +
      "EXCEPT SELECT t1.* FROM t1, t2 where t1.k = t2.k and t1.k != 'one'")
    checkAnswer(df, Seq(Row("one", 3), Row("one", 1)))
  }

  test("literal") {
    val df = sql("SELECT sum(c), max(c), avg(c), count(c), stddev_samp(c) " +
      "FROM (WITH t(c) AS (SELECT 1) SELECT * FROM t)")
    checkAnswer(df, Seq(Row(1, 1, 1, 1, null)))
  }

  test("join with condition") {
    val testData1 = Seq(-234, 145, 367, 975, 298).toDF("int_col1")
    testData1.createOrReplaceTempView("t1")
    val testData2 = Seq(
      (-769, -244),
      (-800, -409),
      (940, 86),
      (-507, 304),
      (-367, 158)).toDF("int_col0", "int_col1")
    testData2.createOrReplaceTempView("t2")

    val df = sql("SELECT (SUM(COALESCE(t1.int_col1, t2.int_col0)))," +
      " ((COALESCE(t1.int_col1, t2.int_col0)) * 2) FROM t1 RIGHT JOIN t2 " +
      "ON (t2.int_col0) = (t1.int_col1) GROUP BY GREATEST(COALESCE(t2.int_col1, 109), " +
      "COALESCE(t1.int_col1, -449)), COALESCE(t1.int_col1, t2.int_col0) HAVING " +
      "(SUM(COALESCE(t1.int_col1, t2.int_col0))) > ((COALESCE(t1.int_col1, t2.int_col0)) * 2)")
    checkAnswer(df, Seq(Row(-367, -734), Row(-769, -1538), Row(-800, -1600), Row(-507, -1014)))
  }

  test("like") {
    Seq(("google", "%oo%"),
       ("facebook", "%oo%"),
       ("linkedin", "%in"))
      .toDF("company", "pat")
      .createOrReplaceTempView("like_all_table")
    val df = sql("SELECT company FROM like_all_table WHERE company LIKE ALL ('%oo%', pat)")
    checkAnswer(df, Seq(Row("google"), Row("facebook")))
  }

  ignore("test2") {
    Seq(1, 3, 5, 7, 9).toDF("id").createOrReplaceTempView("s1")
    Seq(1, 3, 4, 6, 9).toDF("id").createOrReplaceTempView("s2")
    Seq(3, 4, 6, 9).toDF("id").createOrReplaceTempView("s3")
    val df = sql("SELECT s1.id, s2.id FROM s1 " +
      "FULL OUTER JOIN s2 ON s1.id = s2.id AND s1.id NOT IN (SELECT id FROM s3)")
    df.show()
  }

  ignore("SMJ") {
    Seq[(String, Integer, Integer, Long, Double, Double, Double, Timestamp, Date)](
      ("val1a", 6, 8, 10L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-04-04 00:00:00.000"), Date.valueOf("2014-04-04")),
      ("val1b", 8, 16, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")),
      ("val1a", 16, 12, 21L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-06-04 01:02:00.001"), Date.valueOf("2014-06-04")),
      ("val1a", 16, 12, 10L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-07-04 01:01:00.000"), Date.valueOf("2014-07-04")),
      ("val1c", 8, 16, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.001"), Date.valueOf("2014-05-05")),
      ("val1d", null, 16, 22L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:01:00.000"), null),
      ("val1d", null, 16, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-07-04 01:02:00.001"), null),
      ("val1e", 10, null, 25L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-08-04 01:01:00.000"), Date.valueOf("2014-08-04")),
      ("val1e", 10, null, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-09-04 01:02:00.001"), Date.valueOf("2014-09-04")),
      ("val1d", 10, null, 12L, 17.0, 25D, 26E2, Timestamp.valueOf("2015-05-04 01:01:00.000"), Date.valueOf("2015-05-04")),
      ("val1a", 6, 8, 10L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-04-04 01:02:00.001"), Date.valueOf("2014-04-04")),
      ("val1e", 10, null, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")))
      .toDF("t1a", "t1b", "t1c", "t1d", "t1e", "t1f", "t1g", "t1h", "t1i")
      .createOrReplaceTempView("t1")
    Seq[(String, Integer, Integer, Long, Double, Double, Double, Timestamp, Date)](
      ("val2a", 6, 12, 14L, 15, 20D, 20E2, Timestamp.valueOf("2014-04-04 01:01:00.000"), Date.valueOf("2014-04-04")),
      ("val1b", 10, 12, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 8, 16, 119L, 17, 25D, 26E2, Timestamp.valueOf("2015-05-04 01:01:00.000"), Date.valueOf("2015-05-04")),
      ("val1c", 12, 16, 219L, 17, 25D, 26E2, Timestamp.valueOf("2016-05-04 01:01:00.000"), Date.valueOf("2016-05-04")),
      ("val1b", null, 16, 319L, 17, 25D, 26E2, Timestamp.valueOf("2017-05-04 01:01:00.000"), null),
      ("val2e", 8, null, 419L, 17, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:01:00.000"), Date.valueOf("2014-06-04")),
      ("val1f", 19, null, 519L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 10, 12, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:01:00.000"), Date.valueOf("2014-06-04")),
      ("val1b", 8, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-07-04 01:01:00.000"), Date.valueOf("2014-07-04")),
      ("val1c", 12, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-08-04 01:01:00.000"), Date.valueOf("2014-08-05")),
      ("val1e", 8, null, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-09-04 01:01:00.000"), Date.valueOf("2014-09-04")),
      ("val1f", 19, null, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-10-04 01:01:00.000"), Date.valueOf("2014-10-04")),
      ("val1b", null, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), null))
      .toDF("t2a", "t2b", "t2c", "t2d", "t2e", "t2f", "t2g", "t2h", "t2i")
      .createOrReplaceTempView("t2")
    Seq[(String, Integer, Integer, Long, Double, Double, Double, Timestamp, Date)](
      ("val3a", 6, 12, 110L, 15, 20D, 20E2, Timestamp.valueOf("2014-04-04 01:02:00.000"), Date.valueOf("2014-04-04")),
      ("val3a", 6, 12, 10L, 15, 20D, 20E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 10, 12, 219L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 10, 12, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 8, 16, 319L, 17, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:02:00.000"), Date.valueOf("2014-06-04")),
      ("val1b", 8, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-07-04 01:02:00.000"), Date.valueOf("2014-07-04")),
      ("val3c", 17, 16, 519L, 17, 25D, 26E2, Timestamp.valueOf("2014-08-04 01:02:00.000"), Date.valueOf("2014-08-04")),
      ("val3c", 17, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-09-04 01:02:00.000"), Date.valueOf("2014-09-05")),
      ("val1b", null, 16, 419L, 17, 25D, 26E2, Timestamp.valueOf("2014-10-04 01:02:00.000"), null),
      ("val1b", null, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-11-04 01:02:00.000"), null),
      ("val3b", 8, null, 719L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val3b", 8, null, 19L, 17, 25D, 26E2, Timestamp.valueOf("2015-05-04 01:02:00.000"), Date.valueOf("2015-05-04")))
      .toDF("t3a", "t3b", "t3c", "t3d", "t3e", "t3f", "t3g", "t3h", "t3i")
      .createOrReplaceTempView("t3")
    val df = sql("SELECT t1a, t1b FROM t1 WHERE  NOT EXISTS (SELECT (SELECT max(t2b) FROM t2 " +
      "LEFT JOIN t1 ON t2a = t1a WHERE t2c = t3c) dummy FROM t3 WHERE  t3b < (SELECT max(t2b) " +
      "FROM t2 LEFT JOIN t1 ON t2a = t1a WHERE  t2c = t3c) AND t3a = t1a)")
    df.show()
  }

  test("test3") {
    Seq[(Integer, String, Date, Double, Integer)](
      (100, "emp 1", Date.valueOf("2005-01-01"), 100.00D, 10),
      (100, "emp 1", Date.valueOf("2005-01-01"), 100.00D, 10),
      (200, "emp 2", Date.valueOf("2003-01-01"), 200.00D, 10),
      (300, "emp 3", Date.valueOf("2002-01-01"), 300.00D, 20),
      (400, "emp 4", Date.valueOf("2005-01-01"), 400.00D, 30),
      (500, "emp 5", Date.valueOf("2001-01-01"), 400.00D, null),
      (600, "emp 6 - no dept", Date.valueOf("2001-01-01"), 400.00D, 100),
      (700, "emp 7", Date.valueOf("2010-01-01"), 400.00D, 100),
      (800, "emp 8", Date.valueOf("2016-01-01"), 150.00D, 70))
      .toDF("id", "emp_name", "hiredate", "salary", "dept_id")
      .createOrReplaceTempView("EMP")
    Seq[(Integer, String, String)](
      (10, "dept 1", "CA"),
      (20, "dept 2", "NY"),
      (30, "dept 3", "TX"),
      (40, "dept 4 - unassigned", "OR"),
      (50, "dept 5 - unassigned", "NJ"),
      (70, "dept 7", "FL"))
      .toDF("dept_id", "dept_name", "state")
      .createOrReplaceTempView("DEPT")
    Seq[(String, Double)](
      ("emp 1", 10.00D),
      ("emp 1", 20.00D),
      ("emp 2", 300.00D),
      ("emp 2", 100.00D),
      ("emp 3", 300.00D),
      ("emp 4", 100.00D),
      ("emp 5", 1000.00D),
      ("emp 6 - no dept", 500.00D))
      .toDF("emp_name", "bonus_amt")
      .createOrReplaceTempView("BONUS")

    val df = sql("SELECT * FROM emp WHERE  EXISTS " +
      "(SELECT 1 FROM dept WHERE dept.dept_id > 10 AND dept.dept_id < 30)")
    checkAnswer(df, Seq(
      Row(100, "emp 1", Date.valueOf("2005-01-01"), 100.0, 10),
      Row(100, "emp 1", Date.valueOf("2005-01-01"), 100.0, 10),
      Row(200, "emp 2", Date.valueOf("2003-01-01"), 200.0, 10),
      Row(300, "emp 3", Date.valueOf("2002-01-01"), 300.0, 20),
      Row(400, "emp 4", Date.valueOf("2005-01-01"), 400.0, 30),
      Row(500, "emp 5", Date.valueOf("2001-01-01"), 400.0, null),
      Row(600, "emp 6 - no dept", Date.valueOf("2001-01-01"), 400.0, 100),
      Row(700, "emp 7", Date.valueOf("2010-01-01"), 400.0, 100),
      Row(800, "emp 8", Date.valueOf("2016-01-01"), 150.0, 70)))
    val df2 = sql("SELECT * FROM dept WHERE EXISTS (SELECT dept_id, Count(*) FROM emp " +
      "GROUP BY dept_id HAVING EXISTS (SELECT 1 FROM bonus WHERE bonus_amt < Min(emp.salary)))")
    checkAnswer(df2, Seq(
      Row(10, "dept 1", "CA"),
      Row(20, "dept 2", "NY"),
      Row(30, "dept 3", "TX"),
      Row(40, "dept 4 - unassigned", "OR"),
      Row(50, "dept 5 - unassigned", "NJ"),
      Row(70, "dept 7", "FL")))
  }

  ignore("window1") {
    Seq(1).toDF("id").createOrReplaceTempView("t")
    val df = sql("SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as int)) FROM t")
    df.show()
  }

  ignore("window2") {
    Seq(0, 123456, -123456, 2147483647, -2147483647)
      .toDF("f1").createOrReplaceTempView("int4_tbl")
    val df = sql("SELECT SUM(COUNT(f1)) OVER () FROM int4_tbl WHERE f1=42")
    df.show()
  }

  ignore("union - normalization for a very small value") {
    Seq(0.0, -34.84, -1004.30, -1.2345678901234e+200, -1.2345678901234e-200)
      .toDF("f1").createOrReplaceTempView("FLOAT8_TBL")
    val df = sql("SELECT f1 AS five FROM FLOAT8_TBL UNION SELECT f1 FROM FLOAT8_TBL ORDER BY 1")
    checkAnswer(df, Seq(
      Row(-1.2345678901234E200),
      Row(-1004.3),
      Row(-34.84),
      Row(-1.2345678901234E-200),
      Row(0.0)))
  }

  ignore("int4 and int8 exception") {
    Seq(0, 123456, -123456, 2147483647, -2147483647)
      .toDF("f1").createOrReplaceTempView("INT4_TBL")
    val df = sql("SELECT '' AS five, i.f1, i.f1 * smallint('2') AS x FROM INT4_TBL i")
    df.show()
    Seq[(Long, Long)]((123, 456),
      (123, 4567890123456789L),
      (4567890123456789L, 123),
      (4567890123456789L, 4567890123456789L),
      (4567890123456789L, -4567890123456789L))
      .toDF("q1", "q2")
      .createOrReplaceTempView("INT8_TBL")
    val df1 = sql("SELECT '' AS three, q1, q2, q1 * q2 AS multiply FROM INT8_TBL")
    df1.show()
  }

  ignore("udf") {
    val df = sql("SELECT udf(udf(a)) as a FROM (SELECT udf(0) a, udf(0) b " +
      "UNION ALL SELECT udf(SUM(1)) a, udf(CAST(0 AS BIGINT)) b UNION ALL " +
      "SELECT udf(0) a, udf(0) b) T")
    df.show()
  }

  test("two inner joins with condition") {
    spark
      .read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema(
        """
          |unique1 int,
          |unique2 int,
          |two int,
          |four int,
          |ten int,
          |twenty int,
          |hundred int,
          |thousand int,
          |twothousand int,
          |fivethous int,
          |tenthous int,
          |odd int,
          |even int,
          |stringu1 string,
          |stringu2 string,
          |string4 string
        """.stripMargin)
      .load(testFile("test-data/postgresql/tenk.data"))
      .write
      .format("parquet")
      .saveAsTable("tenk1")
    Seq(0, 123456, -123456, 2147483647, -2147483647)
      .toDF("f1").createOrReplaceTempView("INT4_TBL")
    val df = sql("select a.f1, b.f1, t.thousand, t.tenthous from tenk1 t, " +
      "(select sum(f1)+1 as f1 from int4_tbl i4a) a, (select sum(f1) as f1 from int4_tbl i4b) b " +
      "where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous")
    df.show()
  }

  test("min_max") {
    Seq[(String, Integer, Integer, Long, Double, Double, Double, Timestamp, Date)](
      ("val1a", 6, 8, 10L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-04-04 00:00:00.000"), Date.valueOf("2014-04-04")),
      ("val1b", 8, 16, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")),
      ("val1a", 16, 12, 21L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-06-04 01:02:00.001"), Date.valueOf("2014-06-04")),
      ("val1a", 16, 12, 10L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-07-04 01:01:00.000"), Date.valueOf("2014-07-04")),
      ("val1c", 8, 16, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.001"), Date.valueOf("2014-05-05")),
      ("val1d", null, 16, 22L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:01:00.000"), null),
      ("val1d", null, 16, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-07-04 01:02:00.001"), null),
      ("val1e", 10, null, 25L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-08-04 01:01:00.000"), Date.valueOf("2014-08-04")),
      ("val1e", 10, null, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-09-04 01:02:00.001"), Date.valueOf("2014-09-04")),
      ("val1d", 10, null, 12L, 17.0, 25D, 26E2, Timestamp.valueOf("2015-05-04 01:01:00.000"), Date.valueOf("2015-05-04")),
      ("val1a", 6, 8, 10L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-04-04 01:02:00.001"), Date.valueOf("2014-04-04")),
      ("val1e", 10, null, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")))
      .toDF("t1a", "t1b", "t1c", "t1d", "t1e", "t1f", "t1g", "t1h", "t1i")
      .createOrReplaceTempView("t1")
    Seq[(String, Integer, Integer, Long, Double, Double, Double, Timestamp, Date)](
      ("val2a", 6, 12, 14L, 15, 20D, 20E2, Timestamp.valueOf("2014-04-04 01:01:00.000"), Date.valueOf("2014-04-04")),
      ("val1b", 10, 12, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 8, 16, 119L, 17, 25D, 26E2, Timestamp.valueOf("2015-05-04 01:01:00.000"), Date.valueOf("2015-05-04")),
      ("val1c", 12, 16, 219L, 17, 25D, 26E2, Timestamp.valueOf("2016-05-04 01:01:00.000"), Date.valueOf("2016-05-04")),
      ("val1b", null, 16, 319L, 17, 25D, 26E2, Timestamp.valueOf("2017-05-04 01:01:00.000"), null),
      ("val2e", 8, null, 419L, 17, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:01:00.000"), Date.valueOf("2014-06-04")),
      ("val1f", 19, null, 519L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 10, 12, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:01:00.000"), Date.valueOf("2014-06-04")),
      ("val1b", 8, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-07-04 01:01:00.000"), Date.valueOf("2014-07-04")),
      ("val1c", 12, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-08-04 01:01:00.000"), Date.valueOf("2014-08-05")),
      ("val1e", 8, null, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-09-04 01:01:00.000"), Date.valueOf("2014-09-04")),
      ("val1f", 19, null, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-10-04 01:01:00.000"), Date.valueOf("2014-10-04")),
      ("val1b", null, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), null))
      .toDF("t2a", "t2b", "t2c", "t2d", "t2e", "t2f", "t2g", "t2h", "t2i")
      .createOrReplaceTempView("t2")
    Seq[(String, Integer, Integer, Long, Double, Double, Double, Timestamp, Date)](
      ("val3a", 6, 12, 110L, 15, 20D, 20E2, Timestamp.valueOf("2014-04-04 01:02:00.000"), Date.valueOf("2014-04-04")),
      ("val3a", 6, 12, 10L, 15, 20D, 20E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 10, 12, 219L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 10, 12, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 8, 16, 319L, 17, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:02:00.000"), Date.valueOf("2014-06-04")),
      ("val1b", 8, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-07-04 01:02:00.000"), Date.valueOf("2014-07-04")),
      ("val3c", 17, 16, 519L, 17, 25D, 26E2, Timestamp.valueOf("2014-08-04 01:02:00.000"), Date.valueOf("2014-08-04")),
      ("val3c", 17, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-09-04 01:02:00.000"), Date.valueOf("2014-09-05")),
      ("val1b", null, 16, 419L, 17, 25D, 26E2, Timestamp.valueOf("2014-10-04 01:02:00.000"), null),
      ("val1b", null, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-11-04 01:02:00.000"), null),
      ("val3b", 8, null, 719L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val3b", 8, null, 19L, 17, 25D, 26E2, Timestamp.valueOf("2015-05-04 01:02:00.000"), Date.valueOf("2015-05-04")))
      .toDF("t3a", "t3b", "t3c", "t3d", "t3e", "t3f", "t3g", "t3h", "t3i")
      .createOrReplaceTempView("t3")

    val df = sql("SELECT t1a, t1h FROM t1 WHERE  date(t1h) = (SELECT min(t2i) FROM t2)")
    checkAnswer(df, Seq(
      Row("val1a", Timestamp.valueOf("2014-04-04 00:00:00")),
      Row("val1a", Timestamp.valueOf("2014-04-04 01:02:00.001"))))
  }

  test("groupby") {
    Seq[(Integer, java.lang.Boolean)](
      (1, true),
      (1, false),
      (2, true),
      (3, false),
      (3, null),
      (4, null),
      (4, null),
      (5, null),
      (5, true),
      (5, false))
      .toDF("k", "v")
      .createOrReplaceTempView("test_agg")
    val df1 = sql("SELECT COUNT(DISTINCT b), COUNT(DISTINCT b, c) FROM " +
      "(SELECT 1 AS a, 2 AS b, 3 AS c) GROUP BY a")
    checkAnswer(df1, Seq(Row(1, 1)))
    val df2 = sql("SELECT 1 FROM range(10) HAVING true")
    checkAnswer(df2, Seq(Row(1)))
    val df3 = sql("SELECT k, Every(v) AS every FROM test_agg WHERE k = 2 AND v IN (SELECT Any(v)" +
      " FROM test_agg WHERE k = 1) GROUP BY k")
    checkAnswer(df3, Seq(Row(2, true)))
    val df4 = sql("SELECT k, max(v) FROM test_agg GROUP BY k HAVING max(v) = true")
    checkAnswer(df4, Seq(Row(5, true), Row(1, true), Row(2, true)))
    val df5 = sql("SELECT every(v), some(v), any(v), bool_and(v), bool_or(v) " +
      "FROM test_agg WHERE 1 = 0")
    checkAnswer(df5, Seq(Row(null, null, null, null, null)))
    val df6 =
      sql("SELECT every(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg WHERE k = 4")
    checkAnswer(df6, Seq(Row(null, null, null, null, null)))
  }

  test("count with filter") {
    Seq[(Integer, Integer)](
      (1, 1),
      (1, 2),
      (2, 1),
      (2, 2),
      (3, 1),
      (3, 2),
      (null, 1),
      (3, null),
      (null, null))
      .toDF("a", "b")
      .createOrReplaceTempView("testData")
    val df = sql(
      "SELECT COUNT(a) FILTER (WHERE a = 1), COUNT(b) FILTER (WHERE a > 1) FROM testData")
    checkAnswer(df, Seq(Row(2, 4)))
  }

  test("left anti - 1") {
    Seq[(java.lang.Long, Double)](
      (null, 1.0),
      (2L, 3.0),
      (4L, 5.0))
      .toDF("a", "b")
      .createOrReplaceTempView("m")
    Seq[(java.lang.Long, Double)](
      (null, 1.0),
      (2L, 3.0),
      (6L, 7.0))
      .toDF("c", "d")
      .createOrReplaceTempView("s")
    val df = sql("SELECT * FROM m WHERE  a NOT IN (SELECT c FROM s WHERE  d = 1.0)")
    checkAnswer(df, Nil)
  }

  test("left anti - 2") {
    Seq[(Integer, Integer)](
      (1, 1),
      (2, 1),
      (null, 1),
      (1, 3),
      (null, 3),
      (1, null),
      (null, 2))
      .toDF("a1", "a2")
      .createOrReplaceTempView("a")
    Seq[(Integer, Integer, Integer)](
      (1, 1, 2),
      (null, 3, 2),
      (1, null, 2),
      (1, 2, null))
      .toDF("b1", "b2", "b3")
      .createOrReplaceTempView("b")
    val df = sql("SELECT a1, a2 FROM a WHERE a1 NOT IN (SELECT b.b1 FROM b WHERE a.a2 = b.b2)")
    checkAnswer(df, Seq(Row(1, null), Row(2, 1)))
  }

  test("left anti - null handling in two keys") {
    Seq[(Integer, Integer, Integer)](
      (1, 1, 2),
      (2, 1, 3),
      (null, 1, 6),
      (1, 3, 1),
      (null, null, 6),
      (null, 3, 2),
      (1, null, 9),
      (null, 2, 2))
      .toDF("a1", "a2", "a3")
      .createOrReplaceTempView("a")
    Seq[(Integer, Integer, Integer)](
      (1, 1, 2),
      (null, null, 2),
      (null, 3, 2),
      (1, null, 2),
      (1, 2, null))
      .toDF("b1", "b2", "b3")
      .createOrReplaceTempView("b")

    val df = sql("SELECT a1, a2 FROM a ANTI JOIN b ON a.a1 = b.b1 AND a.a2 = b.b2")
    checkAnswer(df, Seq(
      Row(2, 1),
      Row(null, 1),
      Row(1, 3),
      Row(null, null),
      Row(null, 3),
      Row(1, null),
      Row(null, 2)))
  }

  test("left anti - 4") {
    Seq[(Integer, Double)](
      (null, 1.0),
      (2, 3.0),
      (4, 5.0))
      .toDF("a", "b")
      .createOrReplaceTempView("m")
    Seq[(Integer, Double)](
      (null, 1.0),
      (2, 3.0),
      (6, 7.0))
      .toDF("c", "d")
      .createOrReplaceTempView("s")
    val df = sql("SELECT * FROM m WHERE a NOT IN (SELECT c FROM s WHERE  d > 10.0)")
    checkAnswer(df, Seq(
      Row(null, 1.0),
      Row(2, 3.0),
      Row(4, 5.0)))
  }

  test("count, sum and avg") {
    val df = sql("SELECT count(DISTINCT 2), count(DISTINCT 2,3)")
    checkAnswer(df, Seq(
      Row(1, 1)))
    val df1 = sql("select sum(CAST(null AS int)) from range(1,4)")
    checkAnswer(df1, Seq(Row(null)))
    val df2 = sql("select avg(CAST(null AS int)) from range(1,4)")
    checkAnswer(df2, Seq(Row(null)))
  }

  test("exists-aggregate (left anti)") {
    Seq[(Integer, String, Date, Double, Integer)](
      (100, "emp 1", Date.valueOf("2005-01-01"), 100.00D, 10),
      (100, "emp 1", Date.valueOf("2005-01-01"), 100.00D, 10),
      (200, "emp 2", Date.valueOf("2003-01-01"), 200.00D, 10),
      (300, "emp 3", Date.valueOf("2002-01-01"), 300.00D, 20),
      (400, "emp 4", Date.valueOf("2005-01-01"), 400.00D, 30),
      (500, "emp 5", Date.valueOf("2001-01-01"), 400.00D, null),
      (600, "emp 6 - no dept", Date.valueOf("2001-01-01"), 400.00D, 100),
      (700, "emp 7", Date.valueOf("2010-01-01"), 400.00D, 100),
      (800, "emp 8", Date.valueOf("2016-01-01"), 150.00D, 70))
      .toDF("id", "emp_name", "hiredate", "salary", "dept_id")
      .createOrReplaceTempView("EMP")
    Seq[(Integer, String, String)](
      (10, "dept 1", "CA"),
      (20, "dept 2", "NY"),
      (30, "dept 3", "TX"),
      (40, "dept 4 - unassigned", "OR"),
      (50, "dept 5 - unassigned", "NJ"),
      (70, "dept 7", "FL"))
      .toDF("dept_id", "dept_name", "state")
      .createOrReplaceTempView("DEPT")
    val df = sql("SELECT emp_name FROM emp WHERE NOT EXISTS (SELECT max(dept.dept_id) a " +
      "FROM dept WHERE dept.dept_id = emp.dept_id GROUP BY dept.dept_id)")
    val df2 = sql("SELECT max(dept.dept_id) a FROM dept, emp WHERE dept.dept_id = " +
      "emp.dept_id GROUP BY dept.dept_id")
    df2.show()

    checkAnswer(df, Seq(
      Row("emp 5"),
      Row("emp 6 - no dept"),
      Row("emp 7")))
  }

  test("exists") {
    Seq[(Integer, String, Date, Double, Integer)](
      (100, "emp 1", Date.valueOf("2005-01-01"), 100.00D, 10),
      (100, "emp 1", Date.valueOf("2005-01-01"), 100.00D, 10),
      (200, "emp 2", Date.valueOf("2003-01-01"), 200.00D, 10),
      (300, "emp 3", Date.valueOf("2002-01-01"), 300.00D, 20),
      (400, "emp 4", Date.valueOf("2005-01-01"), 400.00D, 30),
      (500, "emp 5", Date.valueOf("2001-01-01"), 400.00D, null),
      (600, "emp 6 - no dept", Date.valueOf("2001-01-01"), 400.00D, 100),
      (700, "emp 7", Date.valueOf("2010-01-01"), 400.00D, 100),
      (800, "emp 8", Date.valueOf("2016-01-01"), 150.00D, 70))
      .toDF("id", "emp_name", "hiredate", "salary", "dept_id")
      .createOrReplaceTempView("EMP")
    Seq[(Integer, String, String)](
      (10, "dept 1", "CA"),
      (20, "dept 2", "NY"),
      (30, "dept 3", "TX"),
      (40, "dept 4 - unassigned", "OR"),
      (50, "dept 5 - unassigned", "NJ"),
      (70, "dept 7", "FL"))
      .toDF("dept_id", "dept_name", "state")
      .createOrReplaceTempView("DEPT")
    Seq[(String, Double)](
      ("emp 1", 10.00D),
      ("emp 1", 20.00D),
      ("emp 2", 300.00D),
      ("emp 2", 100.00D),
      ("emp 3", 300.00D),
      ("emp 4", 100.00D),
      ("emp 5", 1000.00D),
      ("emp 6 - no dept", 500.00D))
      .toDF("emp_name", "bonus_amt")
      .createOrReplaceTempView("BONUS")
  }

  test("union - 1") {
    Seq[Integer](1).toDF("id")
      .createOrReplaceTempView("t")
    val df = sql("SELECT cast(1 as boolean) FROM t UNION SELECT cast(2 as boolean) FROM t")
    checkAnswer(df, Seq(Row(true)))
    val df1 = sql("SELECT stddev_samp('1') FROM t")
    checkAnswer(df1, Seq(Row(null)))
  }

  test("groupingsets") {
    val df = sql("select four, x from (select four, ten, 'foo' as x from tenk1) as t" +
      " group by grouping sets (four, x) having x = 'foo'")
    checkAnswer(df, Seq(Row(null, "foo")))
    val df1 = sql("select four, x || 'x' from (select four, ten, 'foo' as x from tenk1) as t " +
      "group by grouping sets (four, x) order by four")
    checkAnswer(df1, Seq(
      Row(null, "foox"),
      Row(0, null),
      Row(1, null),
      Row(2, null),
      Row(3, null)))
  }

  ignore("in-order-by: different result for timestamp") {
    Seq[(String, Integer, Integer, Long, Double, Double, Double, Timestamp, Date)](
      ("val1a", 6, 8, 10L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-04-04 00:00:00.000"), Date.valueOf("2014-04-04")),
      ("val1b", 8, 16, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")),
      ("val1a", 16, 12, 21L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-06-04 01:02:00.001"), Date.valueOf("2014-06-04")),
      ("val1a", 16, 12, 10L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-07-04 01:01:00.000"), Date.valueOf("2014-07-04")),
      ("val1c", 8, 16, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.001"), Date.valueOf("2014-05-05")),
      ("val1d", null, 16, 22L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:01:00.000"), null),
      ("val1d", null, 16, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-07-04 01:02:00.001"), null),
      ("val1e", 10, null, 25L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-08-04 01:01:00.000"), Date.valueOf("2014-08-04")),
      ("val1e", 10, null, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-09-04 01:02:00.001"), Date.valueOf("2014-09-04")),
      ("val1d", 10, null, 12L, 17.0, 25D, 26E2, Timestamp.valueOf("2015-05-04 01:01:00.000"), Date.valueOf("2015-05-04")),
      ("val1a", 6, 8, 10L, 15.0, 20D, 20E2, Timestamp.valueOf("2014-04-04 01:02:00.001"), Date.valueOf("2014-04-04")),
      ("val1e", 10, null, 19L, 17.0, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")))
      .toDF("t1a", "t1b", "t1c", "t1d", "t1e", "t1f", "t1g", "t1h", "t1i")
      .createOrReplaceTempView("t1")
    Seq[(String, Integer, Integer, Long, Double, Double, Double, Timestamp, Date)](
      ("val2a", 6, 12, 14L, 15, 20D, 20E2, Timestamp.valueOf("2014-04-04 01:01:00.000"), Date.valueOf("2014-04-04")),
      ("val1b", 10, 12, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 8, 16, 119L, 17, 25D, 26E2, Timestamp.valueOf("2015-05-04 01:01:00.000"), Date.valueOf("2015-05-04")),
      ("val1c", 12, 16, 219L, 17, 25D, 26E2, Timestamp.valueOf("2016-05-04 01:01:00.000"), Date.valueOf("2016-05-04")),
      ("val1b", null, 16, 319L, 17, 25D, 26E2, Timestamp.valueOf("2017-05-04 01:01:00.000"), null),
      ("val2e", 8, null, 419L, 17, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:01:00.000"), Date.valueOf("2014-06-04")),
      ("val1f", 19, null, 519L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 10, 12, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:01:00.000"), Date.valueOf("2014-06-04")),
      ("val1b", 8, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-07-04 01:01:00.000"), Date.valueOf("2014-07-04")),
      ("val1c", 12, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-08-04 01:01:00.000"), Date.valueOf("2014-08-05")),
      ("val1e", 8, null, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-09-04 01:01:00.000"), Date.valueOf("2014-09-04")),
      ("val1f", 19, null, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-10-04 01:01:00.000"), Date.valueOf("2014-10-04")),
      ("val1b", null, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:01:00.000"), null))
      .toDF("t2a", "t2b", "t2c", "t2d", "t2e", "t2f", "t2g", "t2h", "t2i")
      .createOrReplaceTempView("t2")
    Seq[(String, Integer, Integer, Long, Double, Double, Double, Timestamp, Date)](
      ("val3a", 6, 12, 110L, 15, 20D, 20E2, Timestamp.valueOf("2014-04-04 01:02:00.000"), Date.valueOf("2014-04-04")),
      ("val3a", 6, 12, 10L, 15, 20D, 20E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 10, 12, 219L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 10, 12, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val1b", 8, 16, 319L, 17, 25D, 26E2, Timestamp.valueOf("2014-06-04 01:02:00.000"), Date.valueOf("2014-06-04")),
      ("val1b", 8, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-07-04 01:02:00.000"), Date.valueOf("2014-07-04")),
      ("val3c", 17, 16, 519L, 17, 25D, 26E2, Timestamp.valueOf("2014-08-04 01:02:00.000"), Date.valueOf("2014-08-04")),
      ("val3c", 17, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-09-04 01:02:00.000"), Date.valueOf("2014-09-05")),
      ("val1b", null, 16, 419L, 17, 25D, 26E2, Timestamp.valueOf("2014-10-04 01:02:00.000"), null),
      ("val1b", null, 16, 19L, 17, 25D, 26E2, Timestamp.valueOf("2014-11-04 01:02:00.000"), null),
      ("val3b", 8, null, 719L, 17, 25D, 26E2, Timestamp.valueOf("2014-05-04 01:02:00.000"), Date.valueOf("2014-05-04")),
      ("val3b", 8, null, 19L, 17, 25D, 26E2, Timestamp.valueOf("2015-05-04 01:02:00.000"), Date.valueOf("2015-05-04")))
      .toDF("t3a", "t3b", "t3c", "t3d", "t3e", "t3f", "t3g", "t3h", "t3i")
      .createOrReplaceTempView("t3")
    val df = sql("SELECT t1a, t1b, t1h FROM t1 WHERE t1c IN (SELECT t2c FROM t2 WHERE t1a = t2a" +
      " ORDER BY t2b DESC nulls first) OR t1h IN (SELECT t2h FROM t2 WHERE  t1h > t2h) " +
      "ORDER  BY t1h DESC nulls last")
    checkAnswer(df, Seq(
      Row("val1c", 8, Timestamp.valueOf("2014-05-04 01:02:00.001")),
      Row("val1b", 8, Timestamp.valueOf("2014-05-04 01:01:00"))))
  }

  test("group_by_ordinal") {
    val df = sql("select a, count(a) from (select 1 as a) tmp group by 1 order by 1")
    checkAnswer(df, Seq(Row(1, 1)))
  }

  test("udf_aggregates_part1") {
    val df = sql("select sum(udf(CAST(null AS Decimal(38,0)))) from range(1,4)")
    df.show()
  }

  test("window_part1 -- window has incorrect result") {
    spark
      .read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema(
        """
          |unique1 int,
          |unique2 int,
          |two int,
          |four int,
          |ten int,
          |twenty int,
          |hundred int,
          |thousand int,
          |twothousand int,
          |fivethous int,
          |tenthous int,
          |odd int,
          |even int,
          |stringu1 string,
          |stringu2 string,
          |string4 string
        """.stripMargin)
      .load(testFile("test-data/postgresql/tenk.data"))
      .write
      .format("parquet")
      .saveAsTable("tenk1")
    val df = sql("SELECT sum(unique1) over (rows between current row and unbounded following)," +
      "unique1, four FROM tenk1 WHERE unique1 < 10")
    checkAnswer(df, Seq(
      Row(0, 0, 0),
      Row(10, 3, 3),
      Row(15, 5, 1),
      Row(23, 8, 0),
      Row(32, 9, 1),
      Row(38, 6, 2),
      Row(39, 1, 1),
      Row(41, 2, 2),
      Row(45, 4, 0),
      Row(7, 7, 3)))
  }

}
