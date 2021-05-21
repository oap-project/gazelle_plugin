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

package org.apache.spark.sql.execution.ui

import java.util
import java.util.{Locale, Properties}
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}
import org.scalatest.BeforeAndAfter

import org.apache.spark.scheduler.{JobFailed, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.kvstore.InMemoryStore

class AllExecutionsPageSuite extends SharedSparkSession with BeforeAndAfter {

  import testImplicits._

  var kvstore: ElementTrackingStore = _

  after {
    if (kvstore != null) {
      kvstore.close()
      kvstore = null
    }
  }

  test("SPARK-27019: correctly display SQL page when event reordering happens") {
    val statusStore = createStatusStore
    val tab = mock(classOf[SQLTab], RETURNS_SMART_NULLS)
    when(tab.sqlStore).thenReturn(statusStore)

    val request = mock(classOf[HttpServletRequest])
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)

    val html = renderSQLPage(request, tab, statusStore).toString().toLowerCase(Locale.ROOT)
    assert(html.contains("failed queries"))
    assert(!html.contains("1970/01/01"))
  }

  test("sorting should be successful") {
    val statusStore = createStatusStore
    val tab = mock(classOf[SQLTab], RETURNS_SMART_NULLS)
    val request = mock(classOf[HttpServletRequest])

    when(tab.sqlStore).thenReturn(statusStore)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    when(request.getParameter("failed.sort")).thenReturn("Duration")
    val map = new util.HashMap[String, Array[String]]()
    map.put("failed.sort", Array("duration"))
    when(request.getParameterMap()).thenReturn(map)
    val html = renderSQLPage(request, tab, statusStore).toString().toLowerCase(Locale.ROOT)
    assert(!html.contains("illegalargumentexception"))
    assert(html.contains("duration"))
  }


  private def createStatusStore: SQLAppStatusStore = {
    val conf = sparkContext.conf
    kvstore = new ElementTrackingStore(new InMemoryStore, conf)
    val listener = new SQLAppStatusListener(conf, kvstore, live = true)
    new SQLAppStatusStore(kvstore, Some(listener))
  }

  private def createTestDataFrame: DataFrame = {
    Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1")
  }

  /**
   * Render a stage page started with the given conf and return the HTML.
   * This also runs a dummy execution page to populate the page with useful content.
   */
  private def renderSQLPage(
    request: HttpServletRequest,
    tab: SQLTab,
    statusStore: SQLAppStatusStore): Seq[Node] = {

    val listener = statusStore.listener.get

    val page = new AllExecutionsPage(tab)
    Seq(0, 1).foreach { executionId =>
      val df = createTestDataFrame
      listener.onOtherEvent(SparkListenerSQLExecutionStart(
        executionId,
        "test",
        "test",
        df.queryExecution.toString,
        SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
        System.currentTimeMillis()))
      listener.onOtherEvent(SparkListenerSQLExecutionEnd(
        executionId, System.currentTimeMillis()))
      listener.onJobStart(SparkListenerJobStart(
        jobId = 0,
        time = System.currentTimeMillis(),
        stageInfos = Nil,
        createProperties(executionId)))
      listener.onJobEnd(SparkListenerJobEnd(
        jobId = 0,
        time = System.currentTimeMillis(),
        JobFailed(new RuntimeException("Oops"))))
    }
    page.render(request)
  }

  private def createProperties(executionId: Long): Properties = {
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
    properties
  }
}

