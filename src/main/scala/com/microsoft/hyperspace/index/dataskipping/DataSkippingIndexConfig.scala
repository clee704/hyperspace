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

import org.apache.spark.sql.DataFrame

import com.microsoft.hyperspace.index.{IndexConfigTrait, IndexerContext}
import com.microsoft.hyperspace.index.dataskipping.sketch.Sketch

case class DataSkippingIndexConfig(override val indexName: String, sketches: Seq[Sketch])
    extends IndexConfigTrait {

  override def referencedColumns: Seq[String] = sketches.flatMap(_.columns)

  override def createIndex(
      ctx: IndexerContext,
      sourceData: DataFrame,
      properties: Map[String, String]): (DataSkippingIndex, DataFrame) = {
    val resolvedSketches = DataSkippingIndex.resolveSketch(sketches, sourceData)
    val sketchValueCounts = resolvedSketches.map(_.numValues)
    val sketchOffsets = sketchValueCounts.scanLeft(0)((offset, numValues) => offset + numValues)
    val index = DataSkippingIndex(resolvedSketches, sketchOffsets, properties)
    (index, index.index(ctx, sourceData))
  }
}
