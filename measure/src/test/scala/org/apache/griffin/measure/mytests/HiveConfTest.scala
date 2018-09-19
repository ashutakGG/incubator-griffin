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
package org.apache.griffin.measure.mytests

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.griffin.measure.Application
import org.scalatest._

class HiveConfTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  "application" should "understand hiveconf" in {
    val envPath = getClass.getResource("/myconf/env.json").getFile
    val dqPath = getClass.getResource("/myconf/dq.json").getFile
    val hiveconfPath = getClass.getResource("/myconf/hiveconf.txt").getFile

    spark.sql(
      "CREATE TABLE IF NOT EXISTS employee " +
      "( eid int, name String," +
      "salary String, destination String) " +
      "ROW FORMAT DELIMITED " +
      "STORED AS TEXTFILE"
    )

    Application.doMain(Array(envPath, dqPath, hiveconfPath))
  }
}
