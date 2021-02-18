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

package com.intel.oap.tpc.ds

import java.io.{File, IOException}

import com.intel.oap.tpc.ds.TPCDSTableGen._
import io.trino.tpcds.Results.constructResults
import io.trino.tpcds._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class TPCDSTableGen(val spark: SparkSession, scale: Double, path: String) extends Serializable {

  def writeParquetTable(name: String, rows: List[Row]): Unit = {
    if (name.equals("dbgen_version")) {
      return
    }
    val schema = name match {
      case "catalog_sales" => catalogSalesSchema
      case "catalog_returns" => catalogReturnsSchema
      case "inventory" => inventorySchema
      case "store_sales" => storeSalesSchema
      case "store_returns" => storeReturnsSchema
      case "web_sales" => webSalesSchema
      case "web_returns" => webReturnsSchema
      case "call_center" => callCenterSchema
      case "catalog_page" => catalogPageSchema
      case "customer" => customerSchema
      case "customer_address" => customerAddressSchema
      case "customer_demographics" => customerDemographicsSchema
      case "date_dim" => dateDimSchema
      case "household_demographics" => householdDemographicsSchema
      case "income_band" => incomeBandSchema
      case "item" => itemSchema
      case "promotion" => promotionSchema
      case "reason" => reasonSchema
      case "ship_mode" => shipModeSchema
      case "store" => storeSchema
      case "time_dim" => timeDimSchema
      case "warehouse" => warehouseSchema
      case "web_page" => webPageSchema
      case "web_site" => webSiteSchema
    }
    writeParquetTable(name, rows, schema)
  }

  private def writeParquetTable(tableName: String, rows: List[Row], schema: StructType): Unit = {
    if (rows.isEmpty) {
      return
    }

    val stringData = spark.range(0L, rows.size, 1L, 1)
        .mapPartitions { itr =>
          val rowItr = rows.iterator
          itr.map { _ =>
            rowItr.next()
          }
        }(RowEncoder(StructType(schema.fields.map(f => StructField(f.name, StringType)))))

    val convertedData = {
      val columns = schema.fields.map { f =>
        new Column(f.name).cast(f.dataType).as(f.name)
      }
      stringData.select(columns: _*)
    }

    convertedData.coalesce(1)
        .write
        .format("parquet")
        .mode("overwrite")
        .save(path + File.separator + tableName)
  }

  def gen(): Unit = {
    val options = new Options()
    options.scale = 0.01D
    val session = options.toSession
    val tableGenerator = new Gen(session)
    Table.getBaseTables.forEach { t =>
      val (p, c): (List[Row], List[Row]) = tableGenerator.generateSparkRows(t)
      writeParquetTable(t.getName, p)
      if (t.hasChild) {
        writeParquetTable(t.getChild.getName, c)
      }
    }

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

object TPCDSTableGen {

  class Gen(session: Session) extends TableGenerator(session) {

    override def generateTable(table: Table): Unit = {
      throw new UnsupportedOperationException
    }

    def generateSparkRows(table: Table): (List[Row], List[Row]) = {
      if (table.isChild && !session.generateOnlyOneTable) {
        return (List.empty, List.empty)
      }
      val parentRows = ListBuffer[Row]()
      val childRows = ListBuffer[Row]()
      try {
        val results = constructResults(table, session)
        for (parentAndChildRows <- results.asScala) {
          if (parentAndChildRows.size > 0) {
            val parentRow = parentAndChildRows.get(0).asScala
            parentRows.append(Row.fromSeq(parentRow))
          }
          if (parentAndChildRows.size > 1) {
            val childRow = parentAndChildRows.get(1).asScala
            childRows.append(Row.fromSeq(childRow))
          }
        }
      } catch {
        case e: IOException =>
          throw new TpcdsException(e.getMessage)
      }
      (parentRows.toList, childRows.toList)
    }
  }

  // generated by script
  private def catalogSalesSchema = {
    StructType(Seq(
      StructField("cs_sold_date_sk", LongType),
      StructField("cs_sold_time_sk", LongType),
      StructField("cs_ship_date_sk", LongType),
      StructField("cs_bill_customer_sk", LongType),
      StructField("cs_bill_cdemo_sk", LongType),
      StructField("cs_bill_hdemo_sk", LongType),
      StructField("cs_bill_addr_sk", LongType),
      StructField("cs_ship_customer_sk", LongType),
      StructField("cs_ship_cdemo_sk", LongType),
      StructField("cs_ship_hdemo_sk", LongType),
      StructField("cs_ship_addr_sk", LongType),
      StructField("cs_call_center_sk", LongType),
      StructField("cs_catalog_page_sk", LongType),
      StructField("cs_ship_mode_sk", LongType),
      StructField("cs_warehouse_sk", LongType),
      StructField("cs_item_sk", LongType),
      StructField("cs_promo_sk", LongType),
      StructField("cs_order_number", LongType),
      StructField("cs_quantity", LongType),
      StructField("cs_wholesale_cost", DoubleType),
      StructField("cs_list_price", DoubleType),
      StructField("cs_sales_price", DoubleType),
      StructField("cs_ext_discount_amt", DoubleType),
      StructField("cs_ext_sales_price", DoubleType),
      StructField("cs_ext_wholesale_cost", DoubleType),
      StructField("cs_ext_list_price", DoubleType),
      StructField("cs_ext_tax", DoubleType),
      StructField("cs_coupon_amt", DoubleType),
      StructField("cs_ext_ship_cost", DoubleType),
      StructField("cs_net_paid", DoubleType),
      StructField("cs_net_paid_inc_tax", DoubleType),
      StructField("cs_net_paid_inc_ship", DoubleType),
      StructField("cs_net_paid_inc_ship_tax", DoubleType),
      StructField("cs_net_profit", DoubleType)
    ))
  }
  private def catalogReturnsSchema = {
    StructType(Seq(
      StructField("cr_returned_date_sk", LongType),
      StructField("cr_returned_time_sk", LongType),
      StructField("cr_item_sk", LongType),
      StructField("cr_refunded_customer_sk", LongType),
      StructField("cr_refunded_cdemo_sk", LongType),
      StructField("cr_refunded_hdemo_sk", LongType),
      StructField("cr_refunded_addr_sk", LongType),
      StructField("cr_returning_customer_sk", LongType),
      StructField("cr_returning_cdemo_sk", LongType),
      StructField("cr_returning_hdemo_sk", LongType),
      StructField("cr_returning_addr_sk", LongType),
      StructField("cr_call_center_sk", LongType),
      StructField("cr_catalog_page_sk", LongType),
      StructField("cr_ship_mode_sk", LongType),
      StructField("cr_warehouse_sk", LongType),
      StructField("cr_reason_sk", LongType),
      StructField("cr_order_number", LongType),
      StructField("cr_return_quantity", LongType),
      StructField("cr_return_amount", DoubleType),
      StructField("cr_return_tax", DoubleType),
      StructField("cr_return_amt_inc_tax", DoubleType),
      StructField("cr_fee", DoubleType),
      StructField("cr_return_ship_cost", DoubleType),
      StructField("cr_refunded_cash", DoubleType),
      StructField("cr_reversed_charge", DoubleType),
      StructField("cr_store_credit", DoubleType),
      StructField("cr_net_loss", DoubleType)
    ))
  }
  private def inventorySchema = {
    StructType(Seq(
      StructField("inv_date_sk", LongType),
      StructField("inv_item_sk", LongType),
      StructField("inv_warehouse_sk", LongType),
      StructField("inv_quantity_on_hand", LongType)
    ))
  }
  private def storeSalesSchema = {
    StructType(Seq(
      StructField("ss_sold_date_sk", LongType),
      StructField("ss_sold_time_sk", LongType),
      StructField("ss_item_sk", LongType),
      StructField("ss_customer_sk", LongType),
      StructField("ss_cdemo_sk", LongType),
      StructField("ss_hdemo_sk", LongType),
      StructField("ss_addr_sk", LongType),
      StructField("ss_store_sk", LongType),
      StructField("ss_promo_sk", LongType),
      StructField("ss_ticket_number", LongType),
      StructField("ss_quantity", LongType),
      StructField("ss_wholesale_cost", DoubleType),
      StructField("ss_list_price", DoubleType),
      StructField("ss_sales_price", DoubleType),
      StructField("ss_ext_discount_amt", DoubleType),
      StructField("ss_ext_sales_price", DoubleType),
      StructField("ss_ext_wholesale_cost", DoubleType),
      StructField("ss_ext_list_price", DoubleType),
      StructField("ss_ext_tax", DoubleType),
      StructField("ss_coupon_amt", DoubleType),
      StructField("ss_net_paid", DoubleType),
      StructField("ss_net_paid_inc_tax", DoubleType),
      StructField("ss_net_profit", DoubleType)
    ))
  }
  private def storeReturnsSchema = {
    StructType(Seq(
      StructField("sr_returned_date_sk", LongType),
      StructField("sr_return_time_sk", LongType),
      StructField("sr_item_sk", LongType),
      StructField("sr_customer_sk", LongType),
      StructField("sr_cdemo_sk", LongType),
      StructField("sr_hdemo_sk", LongType),
      StructField("sr_addr_sk", LongType),
      StructField("sr_store_sk", LongType),
      StructField("sr_reason_sk", LongType),
      StructField("sr_ticket_number", LongType),
      StructField("sr_return_quantity", LongType),
      StructField("sr_return_amt", DoubleType),
      StructField("sr_return_tax", DoubleType),
      StructField("sr_return_amt_inc_tax", DoubleType),
      StructField("sr_fee", DoubleType),
      StructField("sr_return_ship_cost", DoubleType),
      StructField("sr_refunded_cash", DoubleType),
      StructField("sr_reversed_charge", DoubleType),
      StructField("sr_store_credit", DoubleType),
      StructField("sr_net_loss", DoubleType)
    ))
  }
  private def webSalesSchema = {
    StructType(Seq(
      StructField("ws_sold_date_sk", LongType),
      StructField("ws_sold_time_sk", LongType),
      StructField("ws_ship_date_sk", LongType),
      StructField("ws_item_sk", LongType),
      StructField("ws_bill_customer_sk", LongType),
      StructField("ws_bill_cdemo_sk", LongType),
      StructField("ws_bill_hdemo_sk", LongType),
      StructField("ws_bill_addr_sk", LongType),
      StructField("ws_ship_customer_sk", LongType),
      StructField("ws_ship_cdemo_sk", LongType),
      StructField("ws_ship_hdemo_sk", LongType),
      StructField("ws_ship_addr_sk", LongType),
      StructField("ws_web_page_sk", LongType),
      StructField("ws_web_site_sk", LongType),
      StructField("ws_ship_mode_sk", LongType),
      StructField("ws_warehouse_sk", LongType),
      StructField("ws_promo_sk", LongType),
      StructField("ws_order_number", LongType),
      StructField("ws_quantity", LongType),
      StructField("ws_wholesale_cost", DoubleType),
      StructField("ws_list_price", DoubleType),
      StructField("ws_sales_price", DoubleType),
      StructField("ws_ext_discount_amt", DoubleType),
      StructField("ws_ext_sales_price", DoubleType),
      StructField("ws_ext_wholesale_cost", DoubleType),
      StructField("ws_ext_list_price", DoubleType),
      StructField("ws_ext_tax", DoubleType),
      StructField("ws_coupon_amt", DoubleType),
      StructField("ws_ext_ship_cost", DoubleType),
      StructField("ws_net_paid", DoubleType),
      StructField("ws_net_paid_inc_tax", DoubleType),
      StructField("ws_net_paid_inc_ship", DoubleType),
      StructField("ws_net_paid_inc_ship_tax", DoubleType),
      StructField("ws_net_profit", DoubleType)
    ))
  }
  private def webReturnsSchema = {
    StructType(Seq(
      StructField("wr_returned_date_sk", LongType),
      StructField("wr_returned_time_sk", LongType),
      StructField("wr_item_sk", LongType),
      StructField("wr_refunded_customer_sk", LongType),
      StructField("wr_refunded_cdemo_sk", LongType),
      StructField("wr_refunded_hdemo_sk", LongType),
      StructField("wr_refunded_addr_sk", LongType),
      StructField("wr_returning_customer_sk", LongType),
      StructField("wr_returning_cdemo_sk", LongType),
      StructField("wr_returning_hdemo_sk", LongType),
      StructField("wr_returning_addr_sk", LongType),
      StructField("wr_web_page_sk", LongType),
      StructField("wr_reason_sk", LongType),
      StructField("wr_order_number", LongType),
      StructField("wr_return_quantity", LongType),
      StructField("wr_return_amt", DoubleType),
      StructField("wr_return_tax", DoubleType),
      StructField("wr_return_amt_inc_tax", DoubleType),
      StructField("wr_fee", DoubleType),
      StructField("wr_return_ship_cost", DoubleType),
      StructField("wr_refunded_cash", DoubleType),
      StructField("wr_reversed_charge", DoubleType),
      StructField("wr_account_credit", DoubleType),
      StructField("wr_net_loss", DoubleType)
    ))
  }
  private def callCenterSchema = {
    StructType(Seq(
      StructField("cc_call_center_sk", LongType),
      StructField("cc_call_center_id", StringType),
      StructField("cc_rec_start_date", DateType),
      StructField("cc_rec_end_date", DateType),
      StructField("cc_closed_date_sk", LongType),
      StructField("cc_open_date_sk", LongType),
      StructField("cc_name", StringType),
      StructField("cc_class", StringType),
      StructField("cc_employees", LongType),
      StructField("cc_sq_ft", LongType),
      StructField("cc_hours", StringType),
      StructField("cc_manager", StringType),
      StructField("cc_mkt_id", LongType),
      StructField("cc_mkt_class", StringType),
      StructField("cc_mkt_desc", StringType),
      StructField("cc_market_manager", StringType),
      StructField("cc_division", LongType),
      StructField("cc_division_name", StringType),
      StructField("cc_company", LongType),
      StructField("cc_company_name", StringType),
      StructField("cc_street_number", StringType),
      StructField("cc_street_name", StringType),
      StructField("cc_street_type", StringType),
      StructField("cc_suite_number", StringType),
      StructField("cc_city", StringType),
      StructField("cc_county", StringType),
      StructField("cc_state", StringType),
      StructField("cc_zip", StringType),
      StructField("cc_country", StringType),
      StructField("cc_gmt_offset", DoubleType),
      StructField("cc_tax_percentage", DoubleType)
    ))
  }
  private def catalogPageSchema = {
    StructType(Seq(
      StructField("cp_catalog_page_sk", LongType),
      StructField("cp_catalog_page_id", StringType),
      StructField("cp_start_date_sk", LongType),
      StructField("cp_end_date_sk", LongType),
      StructField("cp_department", StringType),
      StructField("cp_catalog_number", LongType),
      StructField("cp_catalog_page_number", LongType),
      StructField("cp_description", StringType),
      StructField("cp_type", StringType)
    ))
  }
  private def customerSchema = {
    StructType(Seq(
      StructField("c_customer_sk", LongType),
      StructField("c_customer_id", StringType),
      StructField("c_current_cdemo_sk", LongType),
      StructField("c_current_hdemo_sk", LongType),
      StructField("c_current_addr_sk", LongType),
      StructField("c_first_shipto_date_sk", LongType),
      StructField("c_first_sales_date_sk", LongType),
      StructField("c_salutation", StringType),
      StructField("c_first_name", StringType),
      StructField("c_last_name", StringType),
      StructField("c_preferred_cust_flag", StringType),
      StructField("c_birth_day", LongType),
      StructField("c_birth_month", LongType),
      StructField("c_birth_year", LongType),
      StructField("c_birth_country", StringType),
      StructField("c_login", StringType),
      StructField("c_email_address", StringType),
      StructField("c_last_review_date", StringType)
    ))
  }
  private def customerAddressSchema = {
    StructType(Seq(
      StructField("ca_address_sk", LongType),
      StructField("ca_address_id", StringType),
      StructField("ca_street_number", StringType),
      StructField("ca_street_name", StringType),
      StructField("ca_street_type", StringType),
      StructField("ca_suite_number", StringType),
      StructField("ca_city", StringType),
      StructField("ca_county", StringType),
      StructField("ca_state", StringType),
      StructField("ca_zip", StringType),
      StructField("ca_country", StringType),
      StructField("ca_gmt_offset", DoubleType),
      StructField("ca_location_type", StringType)
    ))
  }
  private def customerDemographicsSchema = {
    StructType(Seq(
      StructField("cd_demo_sk", LongType),
      StructField("cd_gender", StringType),
      StructField("cd_marital_status", StringType),
      StructField("cd_education_status", StringType),
      StructField("cd_purchase_estimate", LongType),
      StructField("cd_credit_rating", StringType),
      StructField("cd_dep_count", LongType),
      StructField("cd_dep_employed_count", LongType),
      StructField("cd_dep_college_count", LongType)
    ))
  }
  private def dateDimSchema = {
    StructType(Seq(
      StructField("d_date_sk", LongType),
      StructField("d_date_id", StringType),
      StructField("d_date", StringType),
      StructField("d_month_seq", LongType),
      StructField("d_week_seq", LongType),
      StructField("d_quarter_seq", LongType),
      StructField("d_year", LongType),
      StructField("d_dow", LongType),
      StructField("d_moy", LongType),
      StructField("d_dom", LongType),
      StructField("d_qoy", LongType),
      StructField("d_fy_year", LongType),
      StructField("d_fy_quarter_seq", LongType),
      StructField("d_fy_week_seq", LongType),
      StructField("d_day_name", StringType),
      StructField("d_quarter_name", StringType),
      StructField("d_holiday", StringType),
      StructField("d_weekend", StringType),
      StructField("d_following_holiday", StringType),
      StructField("d_first_dom", LongType),
      StructField("d_last_dom", LongType),
      StructField("d_same_day_ly", LongType),
      StructField("d_same_day_lq", LongType),
      StructField("d_current_day", StringType),
      StructField("d_current_week", StringType),
      StructField("d_current_month", StringType),
      StructField("d_current_quarter", StringType),
      StructField("d_current_year", StringType)
    ))
  }
  private def householdDemographicsSchema = {
    StructType(Seq(
      StructField("hd_demo_sk", LongType),
      StructField("hd_income_band_sk", LongType),
      StructField("hd_buy_potential", StringType),
      StructField("hd_dep_count", LongType),
      StructField("hd_vehicle_count", LongType)
    ))
  }
  private def incomeBandSchema = {
    StructType(Seq(
      StructField("ib_income_band_sk", LongType),
      StructField("ib_lower_bound", LongType),
      StructField("ib_upper_bound", LongType)
    ))
  }
  private def itemSchema = {
    StructType(Seq(
      StructField("i_item_sk", LongType),
      StructField("i_item_id", StringType),
      StructField("i_rec_start_date", StringType),
      StructField("i_rec_end_date", StringType),
      StructField("i_item_desc", StringType),
      StructField("i_current_price", DoubleType),
      StructField("i_wholesale_cost", DoubleType),
      StructField("i_brand_id", LongType),
      StructField("i_brand", StringType),
      StructField("i_class_id", LongType),
      StructField("i_class", StringType),
      StructField("i_category_id", LongType),
      StructField("i_category", StringType),
      StructField("i_manufact_id", LongType),
      StructField("i_manufact", StringType),
      StructField("i_size", StringType),
      StructField("i_formulation", StringType),
      StructField("i_color", StringType),
      StructField("i_units", StringType),
      StructField("i_container", StringType),
      StructField("i_manager_id", LongType),
      StructField("i_product_name", StringType)
    ))
  }
  private def promotionSchema = {
    StructType(Seq(
      StructField("p_promo_sk", LongType),
      StructField("p_promo_id", StringType),
      StructField("p_start_date_sk", LongType),
      StructField("p_end_date_sk", LongType),
      StructField("p_item_sk", LongType),
      StructField("p_cost", DoubleType),
      StructField("p_response_target", LongType),
      StructField("p_promo_name", StringType),
      StructField("p_channel_dmail", StringType),
      StructField("p_channel_email", StringType),
      StructField("p_channel_catalog", StringType),
      StructField("p_channel_tv", StringType),
      StructField("p_channel_radio", StringType),
      StructField("p_channel_press", StringType),
      StructField("p_channel_event", StringType),
      StructField("p_channel_demo", StringType),
      StructField("p_channel_details", StringType),
      StructField("p_purpose", StringType),
      StructField("p_discount_active", StringType)
    ))
  }
  private def reasonSchema = {
    StructType(Seq(
      StructField("r_reason_sk", LongType),
      StructField("r_reason_id", StringType),
      StructField("r_reason_desc", StringType)
    ))
  }
  private def shipModeSchema = {
    StructType(Seq(
      StructField("sm_ship_mode_sk", LongType),
      StructField("sm_ship_mode_id", StringType),
      StructField("sm_type", StringType),
      StructField("sm_code", StringType),
      StructField("sm_carrier", StringType),
      StructField("sm_contract", StringType)
    ))
  }
  private def storeSchema = {
    StructType(Seq(
      StructField("s_store_sk", LongType),
      StructField("s_store_id", StringType),
      StructField("s_rec_start_date", StringType),
      StructField("s_rec_end_date", StringType),
      StructField("s_closed_date_sk", LongType),
      StructField("s_store_name", StringType),
      StructField("s_number_employees", LongType),
      StructField("s_floor_space", LongType),
      StructField("s_hours", StringType),
      StructField("s_manager", StringType),
      StructField("s_market_id", LongType),
      StructField("s_geography_class", StringType),
      StructField("s_market_desc", StringType),
      StructField("s_market_manager", StringType),
      StructField("s_division_id", LongType),
      StructField("s_division_name", StringType),
      StructField("s_company_id", LongType),
      StructField("s_company_name", StringType),
      StructField("s_street_number", StringType),
      StructField("s_street_name", StringType),
      StructField("s_street_type", StringType),
      StructField("s_suite_number", StringType),
      StructField("s_city", StringType),
      StructField("s_county", StringType),
      StructField("s_state", StringType),
      StructField("s_zip", StringType),
      StructField("s_country", StringType),
      StructField("s_gmt_offset", DoubleType),
      StructField("s_tax_precentage", DoubleType)
    ))
  }
  private def timeDimSchema = {
    StructType(Seq(
      StructField("t_time_sk", LongType),
      StructField("t_time_id", StringType),
      StructField("t_time", LongType),
      StructField("t_hour", LongType),
      StructField("t_minute", LongType),
      StructField("t_second", LongType),
      StructField("t_am_pm", StringType),
      StructField("t_shift", StringType),
      StructField("t_sub_shift", StringType),
      StructField("t_meal_time", StringType)
    ))
  }
  private def warehouseSchema = {
    StructType(Seq(
      StructField("w_warehouse_sk", LongType),
      StructField("w_warehouse_id", StringType),
      StructField("w_warehouse_name", StringType),
      StructField("w_warehouse_sq_ft", LongType),
      StructField("w_street_number", StringType),
      StructField("w_street_name", StringType),
      StructField("w_street_type", StringType),
      StructField("w_suite_number", StringType),
      StructField("w_city", StringType),
      StructField("w_county", StringType),
      StructField("w_state", StringType),
      StructField("w_zip", StringType),
      StructField("w_country", StringType),
      StructField("w_gmt_offset", DoubleType)
    ))
  }
  private def webPageSchema = {
    StructType(Seq(
      StructField("wp_web_page_sk", LongType),
      StructField("wp_web_page_id", StringType),
      StructField("wp_rec_start_date", DateType),
      StructField("wp_rec_end_date", DateType),
      StructField("wp_creation_date_sk", LongType),
      StructField("wp_access_date_sk", LongType),
      StructField("wp_autogen_flag", StringType),
      StructField("wp_customer_sk", LongType),
      StructField("wp_url", StringType),
      StructField("wp_type", StringType),
      StructField("wp_char_count", LongType),
      StructField("wp_link_count", LongType),
      StructField("wp_image_count", LongType),
      StructField("wp_max_ad_count", LongType)
    ))
  }
  private def webSiteSchema = {
    StructType(Seq(
      StructField("web_site_sk", LongType),
      StructField("web_site_id", StringType),
      StructField("web_rec_start_date", DateType),
      StructField("web_rec_end_date", DateType),
      StructField("web_name", StringType),
      StructField("web_open_date_sk", LongType),
      StructField("web_close_date_sk", LongType),
      StructField("web_class", StringType),
      StructField("web_manager", StringType),
      StructField("web_mkt_id", LongType),
      StructField("web_mkt_class", StringType),
      StructField("web_mkt_desc", StringType),
      StructField("web_market_manager", StringType),
      StructField("web_company_id", LongType),
      StructField("web_company_name", StringType),
      StructField("web_street_number", StringType),
      StructField("web_street_name", StringType),
      StructField("web_street_type", StringType),
      StructField("web_suite_number", StringType),
      StructField("web_city", StringType),
      StructField("web_county", StringType),
      StructField("web_state", StringType),
      StructField("web_zip", StringType),
      StructField("web_country", StringType),
      StructField("web_gmt_offset", StringType),
      StructField("web_tax_percentage", DoubleType)
    ))
  }
}
