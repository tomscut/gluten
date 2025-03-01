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
package io.glutenproject.backendsapi

import io.glutenproject.GlutenConfig
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructField


trait BackendSettingsApi {
  def supportFileFormatRead(format: ReadFileFormat,
                            fields: Array[StructField],
                            partTable: Boolean,
                            paths: Seq[String]): Boolean = false
  def supportExpandExec(): Boolean = false
  def supportSortExec(): Boolean = false
  def supportSortMergeJoinExec(): Boolean = true
  def supportWindowExec(windowFunctions: Seq[NamedExpression]): Boolean = {
    false
  }
  def supportColumnarShuffleExec(): Boolean = {
    GlutenConfig.getConf.enableColumnarShuffle
  }
  def enableJoinKeysRewrite(): Boolean = true
  def supportHashBuildJoinTypeOnLeft: JoinType => Boolean = {
    case _: InnerLike | RightOuter | FullOuter => true
    case _ => false
  }
  def supportHashBuildJoinTypeOnRight: JoinType => Boolean = {
    case _: InnerLike | LeftOuter | FullOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
    case _ => false
  }
  def supportStructType(): Boolean = false
  def fallbackOnEmptySchema(plan: SparkPlan): Boolean = false

  // Whether to fallback aggregate at the same time if its child is fallbacked.
  def fallbackAggregateWithChild(): Boolean = false

  def disableVanillaColumnarReaders(conf: SparkConf): Boolean =
    !conf.getBoolean(GlutenConfig.VANILLA_COLUMNAR_READERS_ENABLED.key,
      GlutenConfig.VANILLA_COLUMNAR_READERS_ENABLED.defaultValue.get)

  def recreateJoinExecOnFallback(): Boolean = false
  def removeHashColumnFromColumnarShuffleExchangeExec(): Boolean = false
  /**
   * A shuffle key may be an expression. We would add a projection for this expression shuffle key
   * and make it into a new column which the shuffle will refer to. But we need to remove it from
   * the result columns from the shuffle.
   */
  def supportShuffleWithProject(outputPartitioning: Partitioning, child: SparkPlan): Boolean = false
  def utilizeShuffledHashJoinHint(): Boolean = false
  def excludeScanExecFromCollapsedStage(): Boolean = false
  def avoidOverwritingFilterTransformer(): Boolean = false
  def fallbackFilterWithoutConjunctiveScan(): Boolean = false
  def rescaleDecimalLiteral(): Boolean = false

  /**
   * Whether to replace sort agg with hash agg., e.g., sort agg will be used in
   * spark's planning for string type input.
   */
  def replaceSortAggWithHashAgg: Boolean = false

  /**
   * Get the config prefix for each backend
   */
  def getBackendConfigPrefix: String

  def rescaleDecimalIntegralExpression(): Boolean = false

  def shuffleSupportedCodec(): Set[String]
}
