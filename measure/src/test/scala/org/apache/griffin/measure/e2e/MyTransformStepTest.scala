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

class MyTransformStepTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  val personConnector = DataConnectorParam(
    conType = "HIVE",
    version = "1.2",
    dataFrameName = null,
    config = Map("table.name" -> "person"),
    preProc = List()
  )
  val personConnector2 = DataConnectorParam(
    conType = "HIVE",
    version = "1.2",
    dataFrameName = null,
    config = Map("table.name" -> "person"),
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
//    AccuracyExpr2DQSteps(getDqContext, expr = Expr(), ruleParam = RuleParam)
    val dqContext: DQContext = getDqContext

    // start id
    val applicationId = spark.sparkContext.applicationId
    dqContext.getSink().start(applicationId)

    // build job
    val dqJob = DQJobBuilder.buildDQJob(dqContext, dqParam.getEvaluateRule)

    dqJob.execute(dqContext)

    for {step <- dqJob.dqSteps} {
      println(s">>>>> step: $step")
      step.execute(dqContext)

      println("spark.sql(\"select * from person_accuracy\").show()")

    }

    print(s">>>>> ctx: $dqContext")

    spark.sql(s"select * from ${dqParam.getEvaluateRule.getRules(0).getOutDfName()}").show()
//
//    println(s">>>>> ${dqJob.dqSteps.size}")
//    println(s">>>>> ${dqJob.dqSteps}")
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

  def run: Try[_] = Try {
    val dqContext: DQContext = getDqContext

    // start id
    val applicationId = spark.sparkContext.applicationId
    dqContext.getSink().start(applicationId)

    // build job
    val dqJob = DQJobBuilder.buildDQJob(dqContext, dqParam.getEvaluateRule)

    // dq job execute
    dqJob.execute(dqContext)

    // clean context
    dqContext.clean()

    // finish
    dqContext.getSink().finish()
  }

  private def getDqContext = {
    // get data sources
    val dataSources = DataSourceFactory.getDataSources(spark, null, dqParam.getDataSources)
    dataSources.foreach(_.init)

    // create dq context
    val dqContext: DQContext = DQContext(
      ContextId(System.currentTimeMillis),
      dqParam.getName,
      dataSources,
      getSinkParams,
      BatchProcessType
    )(spark)
    dqContext
  }

  private def getSinkParams: Seq[SinkParam] = {
    val validSinkTypes = dqParam.getValidSinkTypes
    envParam.getSinkParams.flatMap { sinkParam =>
      if (validSinkTypes.contains(sinkParam.getType)) Some(sinkParam) else None
    }
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
