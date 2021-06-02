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

import java.io.ByteArrayInputStream

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.functions.{array_sort, collect_set, input_file_name, max, min}
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.index.{FileIdTracker, HyperspaceSuite, IndexerContext}
import com.microsoft.hyperspace.index.dataskipping.sketch.{BloomFilterSketch, MinMaxSketch, ValueListSketch}
import com.microsoft.hyperspace.util.FileUtils

class DataSkippingIndexTest extends QueryTest with HyperspaceSuite {

  private val dataPath = new Path(inTempDir("Data"))
  private val indexPath = new Path(inTempDir("Index"))

  private val sparkSession = spark
  private val tracker = new FileIdTracker
  private val ctx = new IndexerContext {
    override def spark: SparkSession = sparkSession
    override def fileIdTracker: FileIdTracker = tracker
    override def indexDataPath: Path = indexPath
  }

  after {
    FileUtils.delete(dataPath)
  }

  test("MinMaxSketch.indexedColumns") {
    assert(MinMaxSketch("A").columns === Seq("A"))
  }

  test("DataSkippingIndexConfig.referencedColumns") {
    assert(
      DataSkippingIndexConfig(
        "MyIndex",
        Seq(MinMaxSketch("A"), MinMaxSketch("B"))).referencedColumns === Seq("A", "B"))
  }

  test("DataSkippingIndex.createIndex with MinMaxSketch") {
    spark.range(100).withColumnRenamed("id", "A").toDF.write.parquet(dataPath.toString)
    val df = spark.read.parquet(dataPath.toString)
    val indexConfig = DataSkippingIndexConfig("MyIndex", Seq(MinMaxSketch("A")))
    val (index, indexData) = indexConfig.createIndex(ctx, df, Map())
    assert(index.sketches === indexConfig.sketches)
    val expectedIndexData = df.groupBy(input_file_name()).agg(min("A"), max("A"))
    checkAnswer(indexData, expectedIndexData)
  }

  test("DataSkippingIndex.createIndex with ValueListSketch") {
    spark.range(100).selectExpr("cast(id / 10 as int) as A").toDF.write.parquet(dataPath.toString)
    val df = spark.read.parquet(dataPath.toString)
    val indexConfig = DataSkippingIndexConfig("MyIndex", Seq(ValueListSketch("A")))
    val (index, indexData) = indexConfig.createIndex(ctx, df, Map())
    assert(index.sketches === indexConfig.sketches)
    val expectedIndexData = df.groupBy(input_file_name()).agg(array_sort(collect_set("A")))
    checkAnswer(indexData, expectedIndexData)
  }

  test("DataSkippingIndex.createIndex with BloomFilterSketch") {
    spark.range(100).withColumnRenamed("id", "A").toDF.write.parquet(dataPath.toString)
    val df = spark.read.parquet(dataPath.toString)
    val indexConfig = DataSkippingIndexConfig("MyIndex", Seq(BloomFilterSketch("A", 0.001, 20)))
    val (index, indexData) = indexConfig.createIndex(ctx, df, Map())
    assert(index.sketches === indexConfig.sketches)
    val valuesAndBloomFilters = indexData
      .collect()
      .map { row =>
        val filePath = row.getString(0)
        val values = spark.read.parquet(filePath).collect().toSeq.map(_.getLong(0))
        val bfData = row.getAs[Array[Byte]](1)
        val bf = BloomFilter.readFrom(new ByteArrayInputStream(bfData))
        (values, bf)
      }
    valuesAndBloomFilters.foreach { case (values, bloomFilter) =>
      val valuesNotContained = (Seq.range(0L, 100L).toSet -- values.toSet).toSeq.take(20)
      checkBloomFilter(bloomFilter, values, valuesNotContained)
    }
  }

  private def checkBloomFilter(
      bf: BloomFilter,
      valuesMightBeContained: Seq[Any],
      valuesNotContained: Seq[Any]): Unit = {
    valuesMightBeContained.foreach { v =>
      assert(bf.mightContain(v) === true, s"bf.mightContain($v) === ${bf.mightContain(v)}")
    }
    valuesNotContained.foreach { v =>
      assert(bf.mightContain(v) === false, s"bf.mightContain($v) === ${bf.mightContain(v)}")
    }
  }
}
