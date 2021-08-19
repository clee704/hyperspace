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

package com.microsoft.hyperspace.index.dataskipping.rule

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.datasources.{FileStatusCache, HadoopFsRelation, InMemoryFileIndex, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.isnull
import org.apache.spark.sql.hyperspace.utils.logicalPlanToDataFrame
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.index.{IndexConstants, IndexLogEntryTags}
import com.microsoft.hyperspace.index.dataskipping.DataSkippingIndex
import com.microsoft.hyperspace.index.dataskipping.util.DataSkippingFileIndex
import com.microsoft.hyperspace.index.plans.logical.IndexHadoopFsRelation
import com.microsoft.hyperspace.index.rules.{ExtractRelation, HyperspaceRule, IndexRankFilter, IndexTypeFilter, QueryPlanIndexFilter}
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.PlanToSelectedIndexMap

object ApplyDataSkippingIndex extends HyperspaceRule {
  protected override val filtersOnQueryPlan: Seq[QueryPlanIndexFilter] =
    IndexTypeFilter[DataSkippingIndex]() :: FilterPlanNodeFilter :: FilterConditionFilter :: Nil

  protected override val indexRanker: IndexRankFilter = FilterRankFilter

  override def applyIndex(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): LogicalPlan = {
    if (indexes.isEmpty) {
      return plan
    }
    plan match {
      case filter @ Filter(_, ExtractRelation(relation)) =>
        val indexLogEntry = indexes(relation.plan)
        val indexDataPred = indexLogEntry
          .getTagValue(plan, IndexLogEntryTags.DATASKIPPING_INDEX_PREDICATE)
          .get
          .getOrElse { return plan }
        val indexDataSchema = indexLogEntry.derivedDataset.asInstanceOf[DataSkippingIndex].schema
        val fileStatusCache = FileStatusCache.getOrCreate(spark)
        val indexDataLoc =
          indexLogEntry.withCachedTag(IndexLogEntryTags.DATASKIPPING_INDEX_FILEINDEX) {
            new InMemoryFileIndex(
              spark,
              indexLogEntry.content.files,
              Map.empty,
              Some(indexDataSchema),
              fileStatusCache)
          }
        val indexDataRel = LogicalRelation(
          new HadoopFsRelation(
            indexDataLoc,
            StructType(Nil),
            indexDataSchema,
            None,
            new ParquetFileFormat,
            Map.empty)(spark))
        val indexData = logicalPlanToDataFrame(spark, indexDataRel)
        val originalFileIndex = relation.plan match {
          case LogicalRelation(HadoopFsRelation(location, _, _, _, _, _), _, _, _) => location
          case _ =>
            indexLogEntry.withCachedTag(
              relation.plan,
              IndexLogEntryTags.DATASKIPPING_SOURCE_FILEINDEX) {
              new InMemoryFileIndex(
                spark,
                relation.rootPaths,
                relation.partitionBasePath
                  .map(PartitioningAwareFileIndex.BASE_PATH_PARAM -> _)
                  .toMap,
                Some(relation.schema),
                fileStatusCache)
            }
        }
        val dataSkippingFileIndex = new DataSkippingFileIndex(
          spark,
          indexData,
          indexDataPred,
          indexLogEntry.fileIdTracker,
          originalFileIndex)
        val newFsRelation = IndexHadoopFsRelation(
          relation.createHadoopFsRelation(
            dataSkippingFileIndex,
            relation.schema,
            relation.options),
          spark,
          indexLogEntry)
        val output = relation.output.map(_.asInstanceOf[AttributeReference])
        filter.copy(child = relation.createLogicalRelation(newFsRelation, output))
      case _ => plan
    }
  }

  override def score(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): Int = {
    if (indexes.isEmpty) {
      return 0
    }
    // Return the lowest score so that covering indexes take precedence over
    // data skipping indexes.
    1
  }
}
