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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources._

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.dataskipping._
import com.microsoft.hyperspace.index.dataskipping.sketch._

class ApplyDataSkippingIndexTest extends DataSkippingSuite {
  import spark.implicits._

  override val numParallelism: Int = 10

  test("applyIndex returns the unmodified plan if no index is given.") {
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val query = sourceData.filter("A = 1")
    val plan = query.queryExecution.optimizedPlan
    assert(ApplyDataSkippingIndex.applyIndex(plan, Map.empty) === plan)
  }

  case class SourceData(df: () => DataFrame, description: String)

  case class Param(
      sourceData: SourceData,
      filter: String,
      sketches: Seq[Sketch],
      numExpectedFiles: Int,
      setup: Option[() => _])

  object Param {
    def apply(
        sourceData: SourceData,
        filter: String,
        sketch: Sketch,
        numExpectedFiles: Int): Param = {
      Param(sourceData, filter, Seq(sketch), numExpectedFiles, None)
    }

    def apply(
        sourceData: SourceData,
        filter: String,
        sketches: Seq[Sketch],
        numExpectedFiles: Int): Param = {
      Param(sourceData, filter, sketches, numExpectedFiles, None)
    }

    def apply(
        sourceData: SourceData,
        filter: String,
        sketch: Sketch,
        numExpectedFiles: Int,
        setup: () => _): Param = {
      Param(sourceData, filter, Seq(sketch), numExpectedFiles, Some(setup))
    }

    def apply(
        sourceData: SourceData,
        filter: String,
        sketches: Seq[Sketch],
        numExpectedFiles: Int,
        setup: () => _): Param = {
      Param(sourceData, filter, sketches, numExpectedFiles, Some(setup))
    }
  }

  def dataI: SourceData =
    SourceData(() => createSourceData(spark.range(100).toDF("A")), "source [A:Int]")

  def dataII: SourceData =
    SourceData(
      () => createSourceData(spark.range(100).selectExpr("id as A", "id * 2 as B")),
      "source [A:Int, B:Int]")

  def dataIN: SourceData =
    SourceData(
      () =>
        createSourceData(
          Seq[Integer](1, 2, null, null, null, null, 7, 8, 9, null, 11, 12, null, 14, null, null,
            17, null, 19, 20).toDF("A")),
      "source [A:Int] with nulls")

  def dataIS: SourceData =
    SourceData(() => createSourceData(spark.range(10).toDF("A")), "source [A:Int] small")

  def dataIIP: SourceData =
    SourceData(
      () =>
        createPartitionedSourceData(
          spark.range(100).selectExpr("cast(id / 10 as int) as A", "id as B"),
          Seq("A")),
      "source [A:Int, B:Int] partitioned")

  def dataD: SourceData =
    SourceData(
      () => createSourceData(spark.range(100).map(_.toDouble).toDF("A")),
      "source [A:Double]")

  def dataDS: SourceData =
    SourceData(
      () =>
        createSourceData(
          Seq(
            0.0,
            1.0,
            1.5,
            Double.NegativeInfinity,
            Double.PositiveInfinity,
            Double.NaN,
            3.14,
            2.718,
            -1.1,
            -0.0).toDF("A")),
      "source [A:Double] small")

  def dataN2: SourceData =
    SourceData(
      () =>
        createSourceData(
          spark.read.json(Seq(
            """{"a": 1, "b": {"a": 0, "c": 2, "d": "x"}}""",
            """{"a": 2, "b": {"a": 0, "c": 3, "d": "y"}}""",
            """{"a": 3, "b": {"a": 1, "c": 4, "d": "x"}}""",
            """{"a": 4, "b": {"a": 2, "c": null, "d": "x"}}""",
            """{"a": 2, "b": {"a": 2, "c": 6, "d": "x"}}""",
            """{"a": 2, "b": {"a": 1, "c": 7, "d": "x"}}""",
            """{"b": {"c": 8, "d": "x"}}""",
            """{"b": {"d": "y"}}""",
            """{"a": 3}""",
            """{"b": {"c": 11}}""").toDS)),
      "source [A:Int, B:[A:Int, C:Int, D: String]]")

  def dataN3: SourceData =
    SourceData(
      () =>
        createSourceData(
          spark.read.json(Seq(
            """{"a": {"b": {"c": 1}}}""",
            """{"a": {"b": {"c": 2}}}""",
            """{"a": {"b": {"c": 3}}}""",
            """{"a": {"b": {"c": null}}}""",
            """{"a": {"b": {"c": 5}}}""",
            """{"a": {"b": {"c": 6}}}""",
            """{"a": {"b": {"c": 7}}}""",
            """{"a": {"b": {"c": 8}}}""",
            """{"a": null}""",
            """{"a": {"b": {"c": 0}}}""").toDS)),
      "source [A:[B:[C:Int]]]")

  def dataB: SourceData =
    SourceData(
      () =>
        createSourceData(
          Seq(
            Array[Byte](0, 0, 0, 0),
            Array[Byte](0, 1, 0, 1),
            Array[Byte](1, 2, 3, 4),
            Array[Byte](5, 6, 7, 8),
            Array[Byte](32, 32, 32, 32),
            Array[Byte](64, 64, 64, 64),
            Array[Byte](1, 1, 1, 1),
            Array[Byte](-128, -128, -128, -128),
            Array[Byte](127, 127, 127, 127),
            Array[Byte](-1, 1, 0, 0)).toDF("A")),
      "source [A:Binary]")

  def dataS: SourceData =
    SourceData(
      () =>
        createSourceData(
          Seq(
            "foo1",
            "foo2000",
            "foo3",
            "foo4",
            "foo5",
            null,
            "foo7",
            "foo8",
            "foo9",
            "baar",
            null)
            .toDF("A")),
      "source [A:String]")

  Seq(
    Param(dataI, "A = 10", MinMaxSketch("A"), 1),
    Param(dataI, "50 = a", MinMaxSketch("A"), 1),
    Param(dataI, "A = -10", MinMaxSketch("a"), 0),
    Param(dataI, "A = 5 + 5", MinMaxSketch("A"), 1),
    Param(dataI, "A = 10 or A = 30", MinMaxSketch("A"), 2),
    Param(dataI, "A <25", MinMaxSketch("A"), 3),
    Param(dataI, "30>A", MinMaxSketch("A"), 3),
    Param(dataI, "31 > A", MinMaxSketch("a"), 4),
    Param(dataI, "A > 25", MinMaxSketch("a"), 8),
    Param(dataI, "28 < A", MinMaxSketch("a"), 8),
    Param(dataI, "29< A", MinMaxSketch("A"), 7),
    Param(dataI, "A <= 25", MinMaxSketch("A"), 3),
    Param(dataI, "29 >= A", MinMaxSketch("A"), 3),
    Param(dataI, "30>=A", MinMaxSketch("A"), 4),
    Param(dataI, "A >= 25", MinMaxSketch("A"), 8),
    Param(dataI, "29 <= A", MinMaxSketch("A"), 8),
    Param(dataI, "30 <= A", MinMaxSketch("A"), 7),
    Param(dataI, "A != 1", MinMaxSketch("A"), 10),
    Param(dataI, "not (A != 1 and A != 10)", MinMaxSketch("A"), 2),
    Param(dataI, "!(!(A = 1))", MinMaxSketch("A"), 1),
    Param(dataI, "!(A < 20)", MinMaxSketch("A"), 8),
    Param(dataI, "not (A not in (1, 2, 3))", MinMaxSketch("A"), 1),
    Param(dataS, "A < 'foo'", MinMaxSketch("A"), 1),
    Param(dataS, "A in ('foo1', 'foo9')", ValueListSketch("A"), 2),
    Param(dataS, "A in ('foo1', 'foo5', 'foo9')", BloomFilterSketch("A", 0.01, 10), 3),
    Param(dataI, "A = 10", ValueListSketch("A"), 1),
    Param(dataI, "10 = A", ValueListSketch("a"), 1),
    Param(dataIS, "A != 5", ValueListSketch("A"), 9),
    Param(dataIS, "5 != A", ValueListSketch("A"), 9),
    Param(dataIN, "a!=9", ValueListSketch("a"), 6),
    Param(dataIN, "9 != A", ValueListSketch("A"), 6),
    Param(dataI, "A != 5", ValueListSketch("A"), 10),
    Param(dataI, "A < 34", ValueListSketch("A"), 4),
    Param(dataI, "34 > A", ValueListSketch("A"), 4),
    Param(dataIN, "A < 9", ValueListSketch("a"), 2),
    Param(dataIN, "9 > A", ValueListSketch("A"), 2),
    Param(dataI, "A = 10", BloomFilterSketch("A", 0.01, 10), 1),
    Param(dataI, "A in (2, 3, 5, 7, 11, 13, 17, 19)", BloomFilterSketch("A", 0.001, 10), 2),
    Param(dataI, "A != 10", BloomFilterSketch("A", 0.001, 10), 10),
    Param(dataI, "a = 10", MinMaxSketch("A"), 1),
    Param(dataI, "A = 10", MinMaxSketch("a"), 1),
    Param(dataI, "A in (2, 3, 10, 99)", ValueListSketch("a"), 3),
    Param(dataI, "A in (1, 2, 3, 10)", MinMaxSketch("A"), 2),
    Param(dataI, "A BETWEEN 27 AND 51", MinMaxSketch("A"), 4),
    Param(dataII, "A = 10 OR B = 50", Seq(MinMaxSketch("A"), MinMaxSketch("B")), 2),
    Param(dataII, "A = 10 or B = 50", Seq(MinMaxSketch("A")), 10),
    Param(dataII, "A = 10 and B = 20", MinMaxSketch("A"), 1),
    Param(dataII, "a = 10 AND b = 20", Seq(MinMaxSketch("A"), MinMaxSketch("B")), 1),
    Param(dataII, "A < 30 and B > 20", MinMaxSketch("A"), 3),
    Param(dataII, "A < 30 and b > 40", Seq(MinMaxSketch("a"), MinMaxSketch("B")), 1),
    Param(dataII, "A = 10 and B = 90", Seq(MinMaxSketch("A"), MinMaxSketch("B")), 0),
    Param(
      dataII,
      "A < 31 and B in (1, 2, 11, 12, 21, 22)",
      Seq(MinMaxSketch("A"), BloomFilterSketch("B", 0.001, 10)),
      2),
    Param(dataIN, "A is not null", MinMaxSketch("A"), 7),
    Param(dataIN, "A = 2", MinMaxSketch("A"), 1),
    Param(dataIIP, "B = 10", MinMaxSketch("B"), 1),
    Param(dataN2, "B.C = 2", MinMaxSketch("B.C"), 1),
    Param(dataN2, "B.c = 2", MinMaxSketch("b.C"), 1),
    Param(dataN2, "b.c < 5", MinMaxSketch("b.c"), 3),
    Param(dataN3, "A.B.C = 2", MinMaxSketch("a.B.C"), 1),
    Param(dataDS, "A = 1.0", MinMaxSketch("A"), 1),
    Param(dataDS, "A <= 1.5", MinMaxSketch("A"), 6),
    Param(dataDS, "A >= 1.5", MinMaxSketch("A"), 5),
    Param(dataD, "A in (1, 2, 3, 10)", MinMaxSketch("A"), 2),
    Param(dataII, "A + B < 100", MinMaxSketch("a+b"), 4),
    Param(
      dataI,
      "F(A) = 10",
      MinMaxSketch("F(A)"),
      1,
      () => spark.udf.register("F", (a: Int) => a * 2)),
    Param(
      dataII,
      "A < 50 and F(A,B) < 20",
      Seq(MinMaxSketch("A"), MinMaxSketch("F(A,B)")),
      2,
      () => spark.udf.register("F", (a: Int, b: Int) => b - a)),
    Param(
      dataI,
      "f(a) < 30",
      MinMaxSketch("F(a)"),
      2,
      () => spark.udf.register("F", (a: Int) => a * 2)),
    Param(
      dataB,
      "F(A)",
      ValueListSketch("f(A)"),
      4,
      () => spark.udf.register("F", (a: Array[Byte]) => a.sum == 0))).foreach {
    case Param(sourceData, filter, sketches, numExpectedFiles, setup) =>
      test(
        s"applyIndex works as expected for ${sourceData.description}: " +
          s"filter=[$filter], sketches=[${sketches.mkString(", ")}], " +
          s"numExpectedFiles=[$numExpectedFiles]") {
        val indexConfig = DataSkippingIndexConfig("ind1", sketches.head, sketches.tail: _*)
        if (setup.nonEmpty) {
          setup.get.apply()
        }
        testApplyIndex(sourceData.df(), filter, indexConfig, numExpectedFiles)
      }
  }

  def testApplyIndex(
      sourceData: DataFrame,
      filter: String,
      indexConfig: DataSkippingIndexConfig,
      numExpectedFiles: Int): Unit = {
    val originalNumFiles = listFiles(dataPath()).filter(isParquet).length
    val query = sourceData.filter(filter)
    val plan = query.queryExecution.optimizedPlan
    val indexLogEntry = createIndexLogEntry(indexConfig, sourceData)
    indexLogEntry.setTagValue(
      plan,
      IndexLogEntryTags.DATASKIPPING_INDEX_DATA_PREDICATE,
      indexLogEntry.derivedDataset
        .asInstanceOf[DataSkippingIndex]
        .convertPredicate(
          spark,
          plan.asInstanceOf[Filter].condition,
          sourceData.queryExecution.optimizedPlan))
    val optimizedPlan = ApplyDataSkippingIndex.applyIndex(
      plan,
      Map(sourceData.queryExecution.optimizedPlan -> indexLogEntry))
    if (originalNumFiles == numExpectedFiles) {
      assert(optimizedPlan === plan)
    } else if (numExpectedFiles == 0) {
      optimizedPlan match {
        case LocalRelation(_, data, _) => assert(data.isEmpty)
        case _ => fail(s"unexpected optimizedPlan: $optimizedPlan")
      }
    } else {
      assert(optimizedPlan !== plan)
      optimizedPlan match {
        case Filter(_, LogicalRelation(relation: HadoopFsRelation, _, _, _)) =>
          assert(relation.inputFiles.length === numExpectedFiles)
          checkAnswer(
            spark.read
              .option("basePath", dataPath().toString)
              .parquet(relation.inputFiles: _*)
              .filter(filter),
            query)
        case _ => fail(s"unexpected optimizedPlan: $optimizedPlan")
      }
    }
  }
}
