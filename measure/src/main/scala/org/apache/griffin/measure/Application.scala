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
package org.apache.griffin.measure

import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.configuration.dqdefinition.reader.ParamReaderFactory
import org.apache.griffin.measure.configuration.dqdefinition.{DQConfig, EnvConfig, GriffinConfig, Param}
import org.apache.griffin.measure.launch.DQApp
import org.apache.griffin.measure.launch.batch.BatchDQApp
import org.apache.griffin.measure.launch.streaming.StreamingDQApp

import scala.collection.convert.Wrappers.MutableMapWrapper
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
  * application entrance
  */
object Application extends Loggable {

  def main(args: Array[String]): Unit = {
    doMain(args)
  }

  def doMain(args: Array[String]) = {
    info(args.toString)
    if (args.length < 2) {
      error("Usage: class <env-param> <dq-param> [<hiveconf-param>]")
      sys.exit(-1)
    }

    val envParamFile = args(0)
    val dqParamFile = args(1)
    val hiveConfFileOpt = if (args.length > 2)
      Option(args(2))
    else
      None

    info(envParamFile)
    info(dqParamFile)

    // read param files
    val envParam = readParamFile[EnvConfig](envParamFile) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    val dqParam = readParamFile[DQConfig](dqParamFile) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    val hiveConfMap = readHiveConf(hiveConfFileOpt) // TODO system.exit(-2)

    val allParam: GriffinConfig = GriffinConfig(envParam, dqParam, hiveConfMap)

    // choose process
    val procType = ProcessType(allParam.getDqConfig.getProcType)
    val dqApp: DQApp = procType match {
      case BatchProcessType => BatchDQApp(allParam)
      case StreamingProcessType => StreamingDQApp(allParam)
      case _ => {
        error(s"${procType} is unsupported process type!")
        sys.exit(-4)
      }
    }

    startup

    // dq app init
    dqApp.init match {
      case Success(_) => {
        info("process init success")
      }
      case Failure(ex) => {
        error(s"process init error: ${ex.getMessage}")
        shutdown
        sys.exit(-5)
      }
    }

    // dq app run
    dqApp.run match {
      case Success(_) => {
        info("process run success")
      }
      case Failure(ex) => {
        error(s"process run error: ${ex.getMessage}")

        if (dqApp.retryable) {
          throw ex
        } else {
          shutdown
          sys.exit(-5)
        }
      }
    }

    // dq app end
    dqApp.close match {
      case Success(_) => {
        info("process end success")
      }
      case Failure(ex) => {
        error(s"process end error: ${ex.getMessage}")
        shutdown
        sys.exit(-5)
      }
    }

    shutdown
  }

  private def readHiveConf(hiveConfFileOpt: Option[String]) = {

    val map = scala.collection.mutable.Map[String, String]()
    if (hiveConfFileOpt.isDefined) {
      for (line <- Source.fromFile(hiveConfFileOpt.get).getLines) {
        val keyVal = line.split("=")
        if (keyVal.length != 2) {
          throw new IllegalArgumentException(s"Wrong config line at hiveconf properties file [filePath: $hiveConfFileOpt.get, line: $line]")
        }

        map(keyVal(0)) = keyVal(1)
      }
    }
    Map(map.toList: _*)

  }

  private def readParamFile[T <: Param](file: String)(implicit m: ClassTag[T]): Try[T] = {
    val paramReader = ParamReaderFactory.getParamReader(file)
    paramReader.readConfig[T]
  }

  private def startup(): Unit = {
  }

  private def shutdown(): Unit = {
  }

}
