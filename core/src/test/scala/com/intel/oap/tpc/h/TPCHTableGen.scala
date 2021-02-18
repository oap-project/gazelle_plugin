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

package com.intel.oap.tpc.h

import java.io.File
import java.sql.Date

import io.trino.tpch._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

class TPCHTableGen(val spark: SparkSession, scale: Double, path: String) extends Serializable {

  // lineitem
  private def lineItemGenerator = { () =>
    new LineItemGenerator(scale, 1, 1)
  }

  private def lineItemSchema = {
    StructType(Seq(
      StructField("l_orderkey", LongType),
      StructField("l_partkey", LongType),
      StructField("l_suppkey", LongType),
      StructField("l_linenumber", IntegerType),
      StructField("l_quantity", LongType),
      StructField("l_extendedprice", DoubleType),
      StructField("l_discount", DoubleType),
      StructField("l_tax", DoubleType),
      StructField("l_returnflag", StringType),
      StructField("l_linestatus", StringType),
      StructField("l_commitdate", DateType),
      StructField("l_receiptdate", DateType),
      StructField("l_shipinstruct", StringType),
      StructField("l_shipmode", StringType),
      StructField("l_comment", StringType),
      StructField("l_shipdate", DateType)
    ))
  }

  private def lineItemParser: LineItem => Row =
    lineItem =>
      Row(
        lineItem.getOrderKey,
        lineItem.getPartKey,
        lineItem.getSupplierKey,
        lineItem.getLineNumber,
        lineItem.getQuantity,
        lineItem.getExtendedPrice,
        lineItem.getDiscount,
        lineItem.getTax,
        lineItem.getReturnFlag,
        lineItem.getStatus,
        Date.valueOf(GenerateUtils.formatDate(lineItem.getCommitDate)),
        Date.valueOf(GenerateUtils.formatDate(lineItem.getReceiptDate)),
        lineItem.getShipInstructions,
        lineItem.getShipMode,
        lineItem.getComment,
        Date.valueOf(GenerateUtils.formatDate(lineItem.getShipDate))
      )

  // customer
  private def customerGenerator = { () =>
    new CustomerGenerator(scale, 1, 1)
  }

  private def customerSchema = {
    StructType(Seq(
      StructField("c_custkey", LongType),
      StructField("c_name", StringType),
      StructField("c_address", StringType),
      StructField("c_nationkey", LongType),
      StructField("c_phone", StringType),
      StructField("c_acctbal", DoubleType),
      StructField("c_comment", StringType),
      StructField("c_mktsegment", StringType)
    ))
  }

  private def customerParser: Customer => Row =
    customer =>
      Row(
        customer.getCustomerKey,
        customer.getName,
        customer.getAddress,
        customer.getNationKey,
        customer.getPhone,
        customer.getAccountBalance,
        customer.getComment,
        customer.getMarketSegment,
      )

  private def rowCountOf[U](itr: java.lang.Iterable[U]): Long = {
    var cnt = 0L
    val iterator = itr.iterator
    while (iterator.hasNext) {
      iterator.next()
      cnt = cnt + 1
    }
    cnt
  }

  // orders
  private def orderGenerator = { () =>
    new OrderGenerator(scale, 1, 1)
  }

  private def orderSchema = {
    StructType(Seq(
      StructField("o_orderkey", LongType),
      StructField("o_custkey", LongType),
      StructField("o_orderstatus", StringType),
      StructField("o_totalprice", DoubleType),
      StructField("o_orderpriority", StringType),
      StructField("o_clerk", StringType),
      StructField("o_shippriority", IntegerType),
      StructField("o_comment", StringType),
      StructField("o_orderdate", DateType)
    ))
  }

  private def orderParser: Order => Row =
    order =>
      Row(
        order.getOrderKey,
        order.getCustomerKey,
        String.valueOf(order.getOrderStatus),
        order.getTotalPrice,
        order.getOrderPriority,
        order.getClerk,
        order.getShipPriority,
        order.getComment,
        Date.valueOf(GenerateUtils.formatDate(order.getOrderDate))
      )

  // partsupp
  private def partSupplierGenerator = { () =>
    new PartSupplierGenerator(scale, 1, 1)
  }

  private def partSupplierSchema = {
    StructType(Seq(
      StructField("ps_partkey", LongType),
      StructField("ps_suppkey", LongType),
      StructField("ps_availqty", IntegerType),
      StructField("ps_supplycost", DoubleType),
      StructField("ps_comment", StringType)
    ))
  }

  private def partSupplierParser: PartSupplier => Row =
    ps =>
      Row(
        ps.getPartKey,
        ps.getSupplierKey,
        ps.getAvailableQuantity,
        ps.getSupplyCost,
        ps.getComment
      )

  // supplier
  private def supplierGenerator = { () =>
    new SupplierGenerator(scale, 1, 1)
  }

  private def supplierSchema = {
    StructType(Seq(
      StructField("s_suppkey", LongType),
      StructField("s_name", StringType),
      StructField("s_address", StringType),
      StructField("s_nationkey", LongType),
      StructField("s_phone", StringType),
      StructField("s_acctbal", DoubleType),
      StructField("s_comment", StringType)
    ))
  }

  private def supplierParser: Supplier => Row =
    s =>
      Row(
        s.getSupplierKey,
        s.getName,
        s.getAddress,
        s.getNationKey,
        s.getPhone,
        s.getAccountBalance,
        s.getComment
      )

  // nation
  private def nationGenerator = { () =>
    new NationGenerator()
  }

  private def nationSchema = {
    StructType(Seq(
      StructField("n_nationkey", LongType),
      StructField("n_name", StringType),
      StructField("n_regionkey", LongType),
      StructField("n_comment", StringType)
    ))
  }

  private def nationParser: Nation => Row =
    nation =>
      Row(
        nation.getNationKey,
        nation.getName,
        nation.getRegionKey,
        nation.getComment
      )

  // part
  private def partGenerator = { () =>
    new PartGenerator(scale, 1, 1)
  }

  private def partSchema = {
    StructType(Seq(
      StructField("p_partkey", LongType),
      StructField("p_name", StringType),
      StructField("p_mfgr", StringType),
      StructField("p_type", StringType),
      StructField("p_size", IntegerType),
      StructField("p_container", StringType),
      StructField("p_retailprice", DoubleType),
      StructField("p_comment", StringType),
      StructField("p_brand", StringType)
    ))
  }

  private def partParser: Part => Row =
    part =>
      Row(
        part.getPartKey,
        part.getName,
        part.getManufacturer,
        part.getType,
        part.getSize,
        part.getContainer,
        part.getRetailPrice,
        part.getComment,
        part.getBrand
      )

  // region
  private def regionGenerator = { () =>
    new RegionGenerator()
  }

  private def regionSchema = {
    StructType(Seq(
      StructField("r_regionkey", LongType),
      StructField("r_name", StringType),
      StructField("r_comment", StringType)
    ))
  }

  private def regionParser: Region => Row =
    region =>
      Row(
        region.getRegionKey,
        region.getName,
        region.getComment
      )

  // gen tpc-h data
  private def generate[U](dir: String, tableName: String, schema: StructType, gen: () => java.lang.Iterable[U],
      parser: U => Row): Unit = {
    spark.range(0, rowCountOf(gen.apply()), 1L, 1)
        .mapPartitions { itr =>
          val lineItem = gen.apply()
          val lineItemItr = lineItem.iterator()
          val rows = itr.map { _ =>
            val item = lineItemItr.next()
            parser(item)
          }
          rows
        }(RowEncoder(schema))
        .write
        .mode(SaveMode.Overwrite)
        .parquet(path + File.separator + tableName)
  }

  def gen(): Unit = {
    generate(path, "lineitem", lineItemSchema, lineItemGenerator, lineItemParser)
    generate(path, "customer", customerSchema, customerGenerator, customerParser)
    generate(path, "orders", orderSchema, orderGenerator, orderParser)
    generate(path, "partsupp", partSupplierSchema, partSupplierGenerator, partSupplierParser)
    generate(path, "supplier", supplierSchema, supplierGenerator, supplierParser)
    generate(path, "nation", nationSchema, nationGenerator, nationParser)
    generate(path, "part", partSchema, partGenerator, partParser)
    generate(path, "region", regionSchema, regionGenerator, regionParser)
    val files = new File(path).listFiles()
    files.foreach(file => {
      println("Creating catalog table: " + file.getName)
      spark.catalog.createTable(file.getName, file.getAbsolutePath, "arrow")
      try {
        spark.catalog.recoverPartitions(file.getName)
      } catch {
        case _: Throwable =>
      }
    })
  }
}
