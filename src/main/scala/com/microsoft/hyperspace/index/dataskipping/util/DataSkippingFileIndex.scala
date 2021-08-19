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

package com.microsoft.hyperspace.index.dataskipping.util

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.functions.isnull
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.index.{FileIdTracker, IndexConstants}

class DataSkippingFileIndex(
    sparkSession: SparkSession,
    indexData: DataFrame,
    private[dataskipping] val indexDataPred: Expression, // exposed for test
    fileIdTracker: FileIdTracker,
    baseFileIndex: FileIndex)
    extends FileIndex
    with Logging {

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val partitions = baseFileIndex.listFiles(partitionFilters, dataFilters)
    import sparkSession.implicits._
    val pathCol = "__path"
    val filesWithId = partitions
      .flatMap(_.files.map(f => (f.getPath.toString, fileIdTracker.addFile(f))))
      .toDF(pathCol, IndexConstants.DATA_FILE_NAME_ID)
    val selectedFiles = filesWithId
      .hint("broadcast")
      .join(indexData, Seq(IndexConstants.DATA_FILE_NAME_ID), "left")
      .filter(isnull(indexData(IndexConstants.DATA_FILE_NAME_ID)) || new Column(indexDataPred))
      .select(pathCol)
      .collect
      .map(_.getString(0))
      .toSet
    val selectedPartitions = partitions
      .map(p => p.copy(files = p.files.filter(f => selectedFiles.contains(f.getPath.toString))))
      .filter(_.files.nonEmpty)
    logDebug(s"selectedPartitions = $selectedPartitions")
    selectedPartitions
  }

  override def rootPaths: Seq[Path] = baseFileIndex.rootPaths

  override def inputFiles: Array[String] = baseFileIndex.inputFiles

  override def refresh(): Unit = baseFileIndex.refresh()

  override def sizeInBytes: Long = baseFileIndex.sizeInBytes

  override def partitionSchema: StructType = baseFileIndex.partitionSchema

  override def metadataOpsTimeNs: Option[Long] = baseFileIndex.metadataOpsTimeNs
}
