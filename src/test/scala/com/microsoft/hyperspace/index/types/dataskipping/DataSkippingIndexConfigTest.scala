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

package com.microsoft.hyperspace.index.types.dataskipping

import java.io.ByteArrayInputStream

import org.apache.spark.sql.functions.{array_sort, collect_set, input_file_name, max, min}
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.IndexConstants
import com.microsoft.hyperspace.index.types.dataskipping.sketch.{BloomFilterSketch, MinMaxSketch, ValueListSketch}

class DataSkippingIndexConfigTest extends DataSkippingSuite {
  test("indexName returns the index name.") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
    assert(indexConfig.indexName === "myIndex")
  }

  test("sketches returns a single sketch.") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
    assert(indexConfig.sketches === Seq(MinMaxSketch("A")))
  }

  test("sketches returns two sketches.") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"), ValueListSketch("B"))
    assert(indexConfig.sketches === Seq(MinMaxSketch("A"), ValueListSketch("B")))
  }

  test("Duplicate sketches are not allowed.") {
    val exception = intercept[HyperspaceException] {
      DataSkippingIndexConfig("myIndex", MinMaxSketch("A"), MinMaxSketch("A"))
    }
    assert(exception.getMessage.contains("MinMax(A) is specified multiple times."))
  }

  test("Duplicate sketches are not allowed after the column resolution.") {
    val sourceData = createSourceData(spark.range(10).toDF("A"))
    val exception = intercept[HyperspaceException] {
      val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"), MinMaxSketch("a"))
      indexConfig.createIndex(ctx, sourceData, Map())
    }
    assert(exception.getMessage.contains("MinMax(A) is specified multiple times."))
  }

  test("referencedColumns returns referenced columns of sketches.") {
    val indexConfig = DataSkippingIndexConfig("MyIndex", MinMaxSketch("A"), MinMaxSketch("B"))
    assert(indexConfig.referencedColumns === Seq("A", "B"))
  }

  test("createIndex works correctly with a MinMaxSketch.") {
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val indexConfig = DataSkippingIndexConfig("MyIndex", MinMaxSketch("A"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    assert(index.sketches === indexConfig.sketches)
    val expectedSketchValues = sourceData
      .groupBy(input_file_name().as(fileNameCol))
      .agg(min("A"), max("A"))
    checkAnswer(indexData, withFileId(expectedSketchValues))
    assert(indexData.columns === Seq(IndexConstants.DATA_FILE_NAME_ID, "min_A_", "max_A_"))
  }

  test("createIndex works correctly with a ValueListSketch.") {
    val sourceData =
      createSourceData(spark.range(100).selectExpr("cast(id / 10 as int) as A").toDF)
    val indexConfig = DataSkippingIndexConfig("MyIndex", ValueListSketch("A"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    assert(index.sketches === indexConfig.sketches)
    val expectedSketchValues = sourceData
      .groupBy(input_file_name().as(fileNameCol))
      .agg(array_sort(collect_set("A")))
    checkAnswer(indexData, withFileId(expectedSketchValues))
    assert(indexData.columns === Seq(IndexConstants.DATA_FILE_NAME_ID, "value_list_A_"))
  }

  test("createIndex works correctly with a BloomFilterSketch.") {
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val indexConfig = DataSkippingIndexConfig("MyIndex", BloomFilterSketch("A", 0.001, 20))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    assert(index.sketches === indexConfig.sketches)
    val valuesAndBloomFilters = indexData
      .collect()
      .map { row =>
        val fileId = row.getAs[Long](IndexConstants.DATA_FILE_NAME_ID)
        val filePath = fileIdTracker.getIdToFileMapping().toMap.apply(fileId)
        val values = spark.read.parquet(filePath).collect().toSeq.map(_.getLong(0))
        val bfData = row.getAs[Array[Byte]]("bloom_filter_A_")
        val bf = BloomFilter.readFrom(new ByteArrayInputStream(bfData))
        (values, bf)
      }
    valuesAndBloomFilters.foreach {
      case (values, bloomFilter) =>
        // Note: Lower FPP if the test fails.
        val valuesNotContained = (Seq.range(0L, 100L).toSet -- values.toSet).toSeq.take(20)
        checkBloomFilter(bloomFilter, values, valuesNotContained)
    }
    assert(indexData.columns === Seq(IndexConstants.DATA_FILE_NAME_ID, "bloom_filter_A_"))
  }

  test("createIndex resolves column names.") {
    val sourceData = createSourceData(spark.range(10).toDF("Foo"))
    val indexConfig = DataSkippingIndexConfig("MyIndex", MinMaxSketch("foO"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    assert(index.sketches === Seq(MinMaxSketch("Foo")))
  }
}
