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

package org.apache.spark.sql.connector

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, Identifier, Table, V1Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

/**
 * A V2SessionCatalog implementation that can be extended to generate arbitrary `Table` definitions
 * for testing DDL as well as write operations (through df.write.saveAsTable, df.write.insertInto
 * and SQL).
 */
private[connector] trait TestV2SessionCatalogBase[T <: Table] extends DelegatingCatalogExtension {

  protected val tables: util.Map[Identifier, T] = new ConcurrentHashMap[Identifier, T]()

  private val tableCreated: AtomicBoolean = new AtomicBoolean(false)

  private def addTable(ident: Identifier, table: T): Unit = {
    tableCreated.set(true)
    tables.put(ident, table)
  }

  protected def newTable(
      name: String,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): T

  override def loadTable(ident: Identifier): Table = {
    if (tables.containsKey(ident)) {
      tables.get(ident)
    } else {
      // Table was created through the built-in catalog
      super.loadTable(ident) match {
        case v1Table: V1Table if v1Table.v1Table.tableType == CatalogTableType.VIEW => v1Table
        case t =>
          val table = newTable(t.name(), t.schema(), t.partitioning(), t.properties())
          addTable(ident, table)
          table
      }
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val created = super.createTable(ident, schema, partitions, properties)
    val t = newTable(created.name(), schema, partitions, properties)
    addTable(ident, t)
    t
  }

  override def dropTable(ident: Identifier): Boolean = {
    tables.remove(ident)
    super.dropTable(ident)
  }

  def clearTables(): Unit = {
    assert(
      tableCreated.get,
      "Tables are not created, maybe didn't use the session catalog code path?")
    tables.keySet().asScala.foreach(super.dropTable)
    tables.clear()
    tableCreated.set(false)
  }
}
