/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.e2e

import com.fasterxml.jackson.annotation.JsonProperty
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest._
import org.apache.griffin.measure.Application
import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.launch.batch.BatchDQApp

class MyTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  "application" should "understand hiveconf" in {
    val envPath = getClass.getResource("/myconf/env.json").getFile
    val dqPath = getClass.getResource("/myconf/dq.json").getFile

    spark.sql(
      "CREATE TABLE IF NOT EXISTS employee " +
        "( eid int, name String," +
        "salary String, destination String) " +
        "ROW FORMAT DELIMITED " +
        "STORED AS TEXTFILE"
    )

    Application.main(Array(envPath, dqPath))
  }

  "accuracy" should "provide matchedFraction" in {
    createPersonsTables

    val personConnector = DataConnectorParam(
      conType = "HIVE",
      version = "1.2",
      dataFrameName = null,
      config = Map("table.name" -> "person2"),
      preProc = List()
    )
    val personConnector2 = DataConnectorParam(
      conType = "HIVE",
      version = "1.2",
      dataFrameName = null,
      config = Map("table.name" -> "person2"),
      preProc = List()
    )
    val dqParam = DQConfig(
      name = "dqParamMain",
      timestamp = 11111L,
      procType = "",
      dataSources = List(
        DataSourceParam(
          name = "source",
          baseline = false,
          connectors = List(personConnector),
          checkpoint = Map()
        ),
        DataSourceParam(
          name = "target",
          baseline = false,
          connectors = List(personConnector2),
          checkpoint = Map()
        )
      ),
      evaluateRule = EvaluateRuleParam(List(
        RuleParam(
          dslType = "griffin-dsl",
          dqType = "ACCURACY",
          outDfName = "person_accuracy",
          rule = "source.name=target.name",
          outputs = List(RuleOutputParam(name = "spark-sql-test-out", outputType = "metric", flatten = "")),
          inDfName = "",
          details = Map(),
          cache = false
        )
      )),
      sinks = List(
        "console",
        "hdfs",
        "elasticsearch"
      )
    )
    val envParam = EnvConfig(
      sparkParam = emptySparkParam,
      sinkParams = List(SinkParam(sinkType = "console", config = Map())),
      checkpointParams = List()
    )

    spark.sql("SELECT * From person").show()


    val app = BatchDQApp(GriffinConfig(envParam, dqParam))

    app.init
    app.run
//
//    app.sqlContext
  }

  "test" should "test" in {
    createPersonsTables

    spark.sql("SELECT count(*) as cnt1, count(*) as cnt2 From person").explain()
    println(">>>>>>")
    spark.sql("select cnt as cnt1, cnt as cnt2 from (SELECT count(*) as cnt From person)").explain()

  }

  private def createPersonsTables = {
    val personCsvPath = getClass.getResource("/hive/person_table.csv").getFile

    // Table 'person'
    spark.sql(
      "CREATE TABLE IF NOT EXISTS person " +
        "( " +
        "  name String," +
        "  age int " +
        ") " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' " +
        "STORED AS TEXTFILE"
    )

    spark.sql(s"LOAD DATA LOCAL INPATH '$personCsvPath' OVERWRITE INTO TABLE person")

    // Table 'person2'
    spark.sql(
      "CREATE TABLE IF NOT EXISTS person2 " +
        "( " +
        "  name String," +
        "  age int " +
        ") " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' " +
        "STORED AS TEXTFILE"
    )

//    spark.sql(s"LOAD DATA LOCAL INPATH '$personCsvPath' OVERWRITE INTO TABLE person2")
  }

  private lazy val emptySparkParam = {
    SparkParam(
      logLevel = "",
      cpDir = "",
      batchInterval = "",
      processInterval = "",
      config = Map(),
      initClear = false
    )
  }
}
