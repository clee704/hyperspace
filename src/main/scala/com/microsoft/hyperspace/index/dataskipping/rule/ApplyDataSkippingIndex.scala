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
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.datasources.{FileStatusCache, InMemoryFileIndex}

import com.microsoft.hyperspace.index.{IndexConstants, IndexLogEntryTags}
import com.microsoft.hyperspace.index.dataskipping.DataSkippingIndex
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
          .getTagValue(plan, IndexLogEntryTags.DATASKIPPING_INDEX_DATA_PREDICATE)
          .get
          .getOrElse { return plan }
        val indexData = spark.read.parquet(indexLogEntry.content.files.map(_.toString): _*)
        indexData.cache()
        val filteredIndexData = indexData.filter(new Column(Not(indexDataPred)))

        val sp = spark
        import sp.implicits._
        val pathCol = "_path"

        // TODO: Improve file handling performance with many files (> 1 million).
        val sourceFiles = relation.allFiles
        val sourceFileIds = sourceFiles
          .map(f => (indexLogEntry.fileIdTracker.addFile(f), f.getPath.toString))
          .toDF(IndexConstants.DATA_FILE_NAME_ID, pathCol)

        val filteredIndexDataWithPaths = sourceFileIds
          .hint("broadcast")
          .join(filteredIndexData, Seq(IndexConstants.DATA_FILE_NAME_ID), "leftanti")
        val filteredSourceFiles = filteredIndexDataWithPaths
          .select(pathCol)
          .collect
          .map(_.getString(0))

        if (filteredSourceFiles.isEmpty) {
          // Return an empty relation.
          LocalRelation(plan.output)
        } else if (filteredSourceFiles.length == sourceFiles.length) {
          // No change to the plan
          plan
        } else {
          val newFsRelation = IndexHadoopFsRelation(
            relation.createHadoopFsRelation(
              new InMemoryFileIndex(
                spark,
                filteredSourceFiles.map(new Path(_)),
                relation.partitionBasePath.map("basePath" -> _).toMap,
                None,
                FileStatusCache.getOrCreate(spark)),
              relation.schema,
              relation.options),
            spark,
            indexLogEntry)
          val output = relation.output.map(_.asInstanceOf[AttributeReference])
          filter.copy(child = relation.createLogicalRelation(newFsRelation, output))
        }
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
