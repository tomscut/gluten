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

package io.glutenproject.benchmarks

import io.glutenproject.execution.{WholeStageTransformer, WholeStageTransformerSuite}

import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.commons.io.FileUtils
import org.scalatest.Tag

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

object GenerateExample extends Tag("io.glutenproject.tags.GenerateExample")

class NativeBenchmarkPlanGenerator extends WholeStageTransformerSuite {
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"
  val generatedPlanDir = getClass.getResource("/").getPath + "../../../generated-native-benchmark/"
  val outputFileFormat = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val dir = new File(generatedPlanDir)
    if (dir.exists()) {
      FileUtils.forceDelete(dir)
    }
    FileUtils.forceMkdir(dir)
    createTPCHNotNullTables()
  }

  test("Test plan json non-empty - AQE off") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark
        .sql("""
               |select * from lineitem
               |""".stripMargin)
      val executedPlan = df.queryExecution.executedPlan
      val lastStageTransformer = executedPlan.find(_.isInstanceOf[WholeStageTransformer])
      assert(lastStageTransformer.nonEmpty)
      var planJson = lastStageTransformer.get.asInstanceOf[WholeStageTransformer].getPlanJson
      assert(planJson.isEmpty)
      executedPlan.execute()
      planJson = lastStageTransformer.get.asInstanceOf[WholeStageTransformer].getPlanJson
      assert(planJson.nonEmpty)
    }
  }

  test("Test plan json non-empty - AQE on") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df = spark
        .sql("""
               |select * from lineitem join orders on l_orderkey = o_orderkey
               |""".stripMargin)
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
      executedPlan.execute()

      val finalPlan = executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      val lastStageTransformer = finalPlan.find(_.isInstanceOf[WholeStageTransformer])
      assert(lastStageTransformer.nonEmpty)
      val planJson = lastStageTransformer.get.asInstanceOf[WholeStageTransformer].getPlanJson
      assert(planJson.nonEmpty)
    }
  }

  test("generate example", GenerateExample) {
    import testImplicits._
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2"
    ) {
      val q4_lineitem = spark
        .sql(s"""
                |select l_orderkey from lineitem where l_commitdate < l_receiptdate
                |""".stripMargin)
      val q4_orders = spark
        .sql(s"""
                |select o_orderkey, o_orderpriority
                |  from orders
                |  where o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01'
                |""".stripMargin)
      q4_lineitem
        .createOrReplaceTempView("q4_lineitem")
      q4_orders
        .createOrReplaceTempView("q4_orders")

      q4_lineitem
        .repartition(1, 'l_orderkey)
        .write
        .format(outputFileFormat)
        .save(generatedPlanDir + "/example_lineitem")
      q4_orders
        .repartition(1, 'o_orderkey)
        .write
        .format(outputFileFormat)
        .save(generatedPlanDir + "/example_orders")

      val df =
        spark.sql("""
                    |select * from q4_orders left semi join q4_lineitem on l_orderkey = o_orderkey
                    |""".stripMargin)

      val executedPlan = df.queryExecution.executedPlan
      executedPlan.execute()

      val finalPlan =
        executedPlan match {
          case aqe: AdaptiveSparkPlanExec =>
            aqe.executedPlan match {
              case s: ShuffleQueryStageExec => s.shuffle.child
              case other => other
            }
          case plan => plan
        }
      val lastStageTransformer = finalPlan.find(_.isInstanceOf[WholeStageTransformer])
      assert(lastStageTransformer.nonEmpty)
      val plan =
        lastStageTransformer.get.asInstanceOf[WholeStageTransformer].getPlanJson.split('\n')

      val exampleJsonFile = Paths.get(generatedPlanDir, "example.json")
      Files.write(exampleJsonFile, plan.toList.asJava, StandardCharsets.UTF_8)
    }
  }
}
