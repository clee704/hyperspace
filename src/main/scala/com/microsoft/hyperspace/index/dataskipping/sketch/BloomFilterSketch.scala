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

package com.microsoft.hyperspace.index.dataskipping.sketch

import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId}
import org.apache.spark.sql.types.DataType

import com.microsoft.hyperspace.index.dataskipping.util._

/**
 * Sketch based on a bloom filter for a given expression.
 *
 * Being a probabilistic structure, it is more efficient in terms of the index
 * data size than [[ValueListSketch]] if the number of distinct values for the
 * expression is large, but can be less efficient in terms of query optimization
 * than [[ValueListSketch]] due to false positives.
 *
 * Users can specify the target false positive rate and the expected number of
 * distinct values per file. These variables determine the size of the bloom
 * filters and thus the size of the index data.
 *
 * @param expr Expression this sketch is based on
 * @param fpp Target false positive rate
 * @param expectedDistinctCountPerFile Expected number of distinct values per file
 */
case class BloomFilterSketch(
    override val expr: String,
    fpp: Double,
    expectedDistinctCountPerFile: Long,
    override val dataType: Option[DataType] = None)
    extends SingleExprSketch[BloomFilterSketch](expr, dataType) {
  override def name: String = "BloomFilter"

  override def toString: String = s"$name($expr, $fpp, $expectedDistinctCountPerFile)"

  override def withNewExpression(newExpr: (String, Option[DataType])): BloomFilterSketch = {
    copy(expr = newExpr._1, dataType = newExpr._2)
  }

  override def aggregateFunctions: Seq[Expression] = {
    BloomFilterAgg(parsedExpr, expectedDistinctCountPerFile, fpp).toAggregateExpression() :: Nil
  }

  override def convertPredicate(
      predicate: Expression,
      sketchValues: Seq[Expression],
      nameMap: Map[ExprId, String],
      resolvedExprs: Seq[Expression]): Option[Expression] = {
    val bf = sketchValues(0)
    val exprMatcher = ExprMatcher(resolvedExprs.head, nameMap)
    val CEqualTo = ColumnEqualToValue(exprMatcher)
    val CIn = ColumnInValues(exprMatcher)
    Option(predicate).collect {
      case CEqualTo(value) => BloomFilterMightContain(bf, value)
      case CIn(values, dataType) => BloomFilterMightContainAny(bf, values)
    }
  }
}
