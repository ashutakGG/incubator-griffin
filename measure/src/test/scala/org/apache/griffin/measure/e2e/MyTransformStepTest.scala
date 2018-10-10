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

import java.util.Date

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.griffin.measure.Application
import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.configuration.enums.BatchProcessType
import org.apache.griffin.measure.context.{ContextId, DQContext}
import org.apache.griffin.measure.datasource.DataSourceFactory
import org.apache.griffin.measure.job.builder.DQJobBuilder
import org.apache.griffin.measure.launch.batch.BatchDQApp
import org.apache.griffin.measure.step.builder.dsl.parser.GriffinDslParser
import org.apache.griffin.measure.step.builder.dsl.transform.AccuracyExpr2DQSteps
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._

import scala.util.Try

case class AccuracyResult(total: Long, miss: Long, matched: Long)

class MyTransformStepTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  import spark.implicits._
//
//  val personConnector = dataConnectorParam(tableName = "person")
//  val personConnector2 = dataConnectorParam(tableName = "person2")
//
  private val envParam = EnvConfig(
    sparkParam = emptySparkParam,
    sinkParams = List(SinkParam(sinkType = "console", config = Map())),
    checkpointParams = List()
  )

  override def beforeAll(): Unit = {
    super.beforeAll()

    createPersonsTables

    spark.sql("SELECT * From person").show()

    spark.conf.set("spark.sql.crossJoin.enabled", "true")
  }


  "accuracy" should "provide matchedFraction" in {
    val dqContext: DQContext = getDqContext(
      dataSourcesParam = List(
        DataSourceParam(
          name = "source",
          connectors = List(dataConnectorParam(tableName = "person"))
        ),
        DataSourceParam(
          name = "target",
          connectors = List(dataConnectorParam(tableName = "person"))
        )
      ))

    val accuracyRule = RuleParam(
      dslType = "griffin-dsl",
      dqType = "ACCURACY",
      outDfName = "person_accuracy",
      rule = "source.name=target.name",
      outputs = List(RuleOutputParam(name = "spark-sql-test-out", outputType = "metric", flatten = "")),
      inDfName = "",
      details = Map(),
      cache = false
    )

    val dqJob = DQJobBuilder.buildDQJob(
      dqContext,
      evaluateRuleParam = EvaluateRuleParam(List(accuracyRule))
    )

    dqJob.execute(dqContext)

    val res = spark
      .sql(s"select * from ${accuracyRule.getOutDfName()}")
      .as[AccuracyResult]
      .collect()

    res.length shouldBe 1

    res(0) shouldEqual AccuracyResult(2, 0, 2)
  }

  private def createPersonsTables = {
    val personCsvPath = getClass.getResource("/myconf/hive/person_table.csv").getFile

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

  private def getDqContext(dataSourcesParam: Seq[DataSourceParam], name: String = "test-context"): DQContext = {
    // get data sources
    val dataSources = DataSourceFactory.getDataSources(spark, null, dataSourcesParam)
    dataSources.foreach(_.init)

    // create dq context
    DQContext(
      ContextId(System.currentTimeMillis),
      name,
      dataSources,
      Nil,
      BatchProcessType
    )(spark)
  }

  private def dataConnectorParam(tableName: String) = {
    DataConnectorParam(
      conType = "HIVE",
      version = null,
      dataFrameName = null,
      config = Map("table.name" -> tableName),
      preProc = null
    )
  }
}


//
//List(
//  SeqDQStep(
//    List(
//      SparkSqlTransformStep(__missRecords,SELECT `source`.* FROM `source` LEFT JOIN `target` ON coalesce(`source`.`name`, '') = coalesce(`target`.`name`, '') WHERE (NOT (`source`.`name` IS NULL)) AND (`target`.`name` IS NULL),Map(),true),
//      SparkSqlTransformStep(__missCount,SELECT COUNT(*) AS `miss` FROM `__missRecords`,Map(),false), SparkSqlTransformStep(__totalCount,SELECT COUNT(*) AS `total` FROM `source`,Map(),false),
//      SparkSqlTransformStep(
//        person_accuracy,
//        SELECT `total`,
//        `miss`,
//        (`total` - `miss`) AS `matched`,
//        ((`__totalCount`.`total` - coalesce(`__missCount`.`miss`, 0)) / `__totalCount`.`total` ) AS `matchedFraction`
//        FROM (
//        SELECT `__totalCount`.`total` AS `total`,
//        coalesce(`__missCount`.`miss`, 0) AS `miss`
//        FROM `__totalCount` LEFT JOIN `__missCount`
//        )
//        ,Map(),false
//      ),
//      MetricWriteStep(spark-sql-test-out,person_accuracy,DefaultFlattenType,None),
//      RecordWriteStep(__missRecords,__missRecords,None,None)
//    )
//  ),
//  MetricFlushStep()
//)
