/*
 * Copyright (2021) The Hyperspace Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.hyperspace.index.dataskipping

import org.apache.spark.sql._
import org.apache.spark.sql.functions.input_file_name

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.dataskipping.sketch.Sketch

case class DataSkippingIndex(
    sketches: Seq[Sketch],
    sketchOffsets: Seq[Int],
    override val properties: Map[String, String])
    extends Index {
  override def kind: String = DataSkippingIndex.kind

  override def kindAbbr: String = DataSkippingIndex.kindAbbr

  override def indexedColumns: Seq[String] = sketches.flatMap(_.columns)

  override def referencedColumns: Seq[String] = indexedColumns

  override def withNewProperties(newProperties: Map[String, String]): DataSkippingIndex = {
    copy(properties = newProperties)
  }

  override def statistics(extended: Boolean): Map[String, String] = {
    Map("sketches" -> sketches.mkString(", "))
  }

  override def canHandleDeletedFiles: Boolean = true

  override def write(ctx: IndexerContext, indexData: DataFrame): Unit = {
    indexData.write.mode(SaveMode.Overwrite).parquet(ctx.indexDataPath.toString)
  }

  override def optimize(ctx: IndexerContext, indexDataFilesToOptimize: Seq[FileInfo]): Unit = {
    val indexData = ctx.spark.read.parquet(indexDataFilesToOptimize.map(_.name): _*)
    write(ctx, indexData)
  }

  override def refreshIncremental(
      ctx: IndexerContext,
      appendedSourceData: Option[DataFrame],
      deletedSourceDataFiles: Seq[FileInfo],
      indexContent: Content): (Index, Index.UpdateMode) = {
    if (appendedSourceData.nonEmpty) {
      write(ctx, index(ctx, appendedSourceData.get))
    }
    (this, Index.UpdateMode.Merge)
  }

  override def refreshFull(
      ctx: IndexerContext,
      sourceData: DataFrame): (DataSkippingIndex, DataFrame) = {
    val updatedIndex = copy(sketches = DataSkippingIndex.resolveSketch(sketches, sourceData))
    (updatedIndex, updatedIndex.index(ctx, sourceData))
  }

  /**
   * Creates index data for the given source data.
   */
  def index(ctx: IndexerContext, sourceData: DataFrame): DataFrame = {
    val aggregateFunctions = sketches.flatMap { s =>
      val aggrs = s.aggregateFunctions
      assert(aggrs.nonEmpty && aggrs.length == s.numValues)
      aggrs
    }
    val fileNameCol = "input_file_name"
    val indexData = sourceData
      .groupBy(input_file_name().as(fileNameCol))
      .agg(aggregateFunctions.head, aggregateFunctions.tail: _*)

    // Get the lineage dataframe to convert file names to file ids
    val spark = ctx.spark
    val relation = RelationUtils.getRelation(spark, sourceData.queryExecution.optimizedPlan)
    val lineagePairs = ctx.fileIdTracker.lineagePairs(relation.filePathNormalizer)
    import spark.implicits._
    val lineageDF = lineagePairs.toDF(fileNameCol, IndexConstants.DATA_FILE_NAME_ID)

    indexData
      .join(lineageDF.hint("broadcast"), fileNameCol)
      .drop(fileNameCol)
  }
}

object DataSkippingIndex {
  final val kind = "DataSkippingIndex"
  final val kindAbbr = "DS"

  /**
   * Returns a copy of the given sketches with the indexed columns replaced by
   * resolved column names.
   */
  def resolveSketch(sketches: Seq[Sketch], sourceData: DataFrame): Seq[Sketch] = {
    sketches.map(s =>
      s.withNewColumns(
        IndexUtils.resolveColumns(sourceData, s.columns).map(_.name)))
  }
}
