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

import scala.collection.mutable

import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal, NamedExpression, Or}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.{input_file_name, min, spark_partition_id}

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.dataskipping.sketch.Sketch
import com.microsoft.hyperspace.index.dataskipping.util.ExpressionUtils
import com.microsoft.hyperspace.util.HyperspaceConf

/**
 * DataSkippingIndex is an index that can accelerate queries by filtering out
 * files in relations using sketches.
 *
 * @param sketches List of sketches for this index
 * @param properties Properties for this index; see [[Index.properties]] for details.
 */
case class DataSkippingIndex(
    sketches: Seq[Sketch],
    override val properties: Map[String, String] = Map.empty)
    extends Index {
  assert(sketches.nonEmpty, "At least one sketch is required.")

  /**
   * Sketch offsets are used to map each sketch to its corresponding columns
   * in the dataframe.
   */
  @transient
  lazy val sketchOffsets: Seq[Int] = sketches.map(_.aggregateFunctions.length).scanLeft(0)(_ + _)

  @transient
  lazy val aggregateFunctions = sketches.flatMap { s =>
    val aggrs = s.aggregateFunctions
    assert(aggrs.nonEmpty)
    aggrs.zipWithIndex.map {
      case (aggr, idx) =>
        new Column(aggr).as(getNormalizeColumnName(s"${s}_$idx"))
    }
  }

  override def kind: String = DataSkippingIndex.kind

  override def kindAbbr: String = DataSkippingIndex.kindAbbr

  override def indexedColumns: Seq[String] = sketches.flatMap(_.indexedColumns).distinct

  override def referencedColumns: Seq[String] = sketches.flatMap(_.referencedColumns).distinct

  override def withNewProperties(newProperties: Map[String, String]): DataSkippingIndex = {
    copy(properties = newProperties)
  }

  override def statistics(extended: Boolean = false): Map[String, String] = {
    Map("sketches" -> sketches.mkString(", "))
  }

  override def canHandleDeletedFiles: Boolean = true

  override def write(ctx: IndexerContext, indexData: DataFrame): Unit = {
    writeImpl(ctx, indexData, SaveMode.Overwrite)
  }

  private def writeImpl(ctx: IndexerContext, indexData: DataFrame, writeMode: SaveMode): Unit = {
    indexData.cache()
    val minRowCountPerFileData = indexData
      .groupBy(spark_partition_id())
      .count()
      .agg(min("count"))
      .collect()
    val minRowCountPerFileConfig =
      HyperspaceConf.DataSkipping.minRecordsPerIndexDataFile(ctx.spark)
    val repartitionedIndexData =
      if (minRowCountPerFileData.nonEmpty &&
        !minRowCountPerFileData.head.isNullAt(0) &&
        minRowCountPerFileData.head.getLong(0) < minRowCountPerFileConfig) {
        val numFiles = math.max(1, indexData.count() / minRowCountPerFileConfig)
        indexData.repartition(numFiles.toInt)
      } else {
        indexData
      }
    repartitionedIndexData.write.mode(writeMode).parquet(ctx.indexDataPath.toString)
  }

  override def optimize(ctx: IndexerContext, indexDataFilesToOptimize: Seq[FileInfo]): Unit = {
    val indexData = ctx.spark.read.parquet(indexDataFilesToOptimize.map(_.name): _*)
    writeImpl(ctx, indexData, SaveMode.Overwrite)
  }

  override def refreshIncremental(
      ctx: IndexerContext,
      appendedSourceData: Option[DataFrame],
      deletedSourceDataFiles: Seq[FileInfo],
      indexContent: Content): (Index, Index.UpdateMode) = {
    if (appendedSourceData.nonEmpty) {
      writeImpl(ctx, index(ctx, appendedSourceData.get), SaveMode.Overwrite)
    }
    if (deletedSourceDataFiles.nonEmpty) {
      val spark = ctx.spark
      import spark.implicits._
      val deletedFileIds = deletedSourceDataFiles.map(_.id).toDF(IndexConstants.DATA_FILE_NAME_ID)
      val updatedIndexData = ctx.spark.read
        .parquet(indexContent.files.map(_.toString): _*)
        .join(deletedFileIds, Seq(IndexConstants.DATA_FILE_NAME_ID), "left_anti")
      val writeMode = if (appendedSourceData.nonEmpty) {
        SaveMode.Append
      } else {
        SaveMode.Overwrite
      }
      writeImpl(ctx, updatedIndexData, writeMode)
    }
    val updateMode = if (deletedSourceDataFiles.isEmpty) {
      Index.UpdateMode.Merge
    } else {
      Index.UpdateMode.Overwrite
    }
    (this, updateMode)
  }

  override def refreshFull(
      ctx: IndexerContext,
      sourceData: DataFrame): (DataSkippingIndex, DataFrame) = {
    val updatedIndex = copy(sketches = ExpressionUtils.resolve(ctx.spark, sketches, sourceData))
    (updatedIndex, updatedIndex.index(ctx, sourceData))
  }

  /**
   * Creates index data for the given source data.
   */
  def index(ctx: IndexerContext, sourceData: DataFrame): DataFrame = {
    val fileNameCol = "input_file_name"
    val indexDataWithFileName = sourceData
      .groupBy(input_file_name().as(fileNameCol))
      .agg(aggregateFunctions.head, aggregateFunctions.tail: _*)

    // Construct a dataframe to convert file names to file ids.
    val spark = ctx.spark
    val relation = RelationUtils.getRelation(spark, sourceData.queryExecution.optimizedPlan)
    import spark.implicits._
    val fileIdDf = ctx.fileIdTracker
      .getIdToFileMapping(relation.pathNormalizer)
      .toDF(IndexConstants.DATA_FILE_NAME_ID, fileNameCol)

    // Drop the file name column and reorder the columns
    // so that the file id column comes first.
    indexDataWithFileName
      .join(fileIdDf.hint("broadcast"), fileNameCol)
      .select(
        IndexConstants.DATA_FILE_NAME_ID,
        indexDataWithFileName.columns.map(c => s"`$c`"): _*)
      .drop(fileNameCol)
  }

  /**
   * Converts the given predicate for the source to a predicate that can be used
   * to filter out unnecessary source data files when applied to index data.
   */
  def convertPredicate(
      spark: SparkSession,
      predicate: Expression,
      source: LogicalPlan): Option[Expression] = {
    val resolvedExprs =
      ExpressionUtils.getResolvedExprs(spark, sketches, source).getOrElse { return None }
    val predMap = buildPredicateMap(spark, predicate, source, resolvedExprs)

    // Creates a single index predicate for a single source predicate node,
    // by combining individual index predicates with And.
    // True is returned if there are no index predicates for the source predicate node.
    def toIndexPred(sourcePred: Expression): Expression = {
      predMap.get(sourcePred).map(_.reduceLeft(And)).getOrElse(Literal.TrueLiteral)
    }

    // Composes an index predicate visiting the source predicate tree recursively.
    def composeIndexPred(sourcePred: Expression): Expression =
      sourcePred match {
        case and: And => And(toIndexPred(and), and.mapChildren(composeIndexPred))
        case or: Or => And(toIndexPred(or), or.mapChildren(composeIndexPred))
        case leaf => toIndexPred(leaf)
      }

    val indexPredicate = composeIndexPred(predicate)

    // Apply constant folding to get the final predicate.
    val optimizePredicate: PartialFunction[Expression, Expression] = {
      case And(Literal.TrueLiteral, right) => right
      case And(left, Literal.TrueLiteral) => left
      case Or(t @ Literal.TrueLiteral, right) => t
      case Or(left, t @ Literal.TrueLiteral) => t
    }
    val optimizedIndexPredicate = indexPredicate.transformUp(optimizePredicate)

    // Return None if the index predicate is True - meaning no conversion can be done.
    if (optimizedIndexPredicate == Literal.TrueLiteral) {
      None
    } else {
      Some(optimizedIndexPredicate)
    }
  }

  // Collect index predicates for each node in the source predicate.
  private def buildPredicateMap(
      spark: SparkSession,
      predicate: Expression,
      source: LogicalPlan,
      resolvedExprs: Map[Sketch, Seq[Expression]])
      : mutable.Map[Expression, mutable.Buffer[Expression]] = {
    val predMap = mutable.Map[Expression, mutable.Buffer[Expression]]()
    predicate.foreachUp { sourcePred =>
      val indexPreds = sketches.zipWithIndex.flatMap {
        case (sketch, idx) =>
          sketch.convertPredicate(
            sourcePred,
            aggrNames(idx).map(UnresolvedAttribute.quoted(_)),
            source.output.map(attr => attr.exprId -> attr.name).toMap,
            resolvedExprs(sketch))
      }
      if (indexPreds.nonEmpty) {
        predMap.getOrElseUpdate(sourcePred, mutable.Buffer.empty) ++= indexPreds
      }
    }
    predMap
  }

  private def aggrNames(i: Int): Seq[String] = {
    aggregateFunctions
      .slice(sketchOffsets(i), sketchOffsets(i + 1))
      .map(_.expr.asInstanceOf[NamedExpression].name)
  }

  /**
   * Returns a normalized column name valid for a Parquet format.
   */
  private def getNormalizeColumnName(name: String): String = {
    name.replaceAll("[ ,;{}()\n\t=]", "_")
  }

  override def equals(that: Any): Boolean =
    that match {
      case DataSkippingIndex(thatSketches, _) => sketches.toSet == thatSketches.toSet
      case _ => false
    }

  override def hashCode: Int = sketches.map(_.hashCode).sum
}

object DataSkippingIndex {
  final val kind = "DataSkippingIndex"
  final val kindAbbr = "DS"
}
