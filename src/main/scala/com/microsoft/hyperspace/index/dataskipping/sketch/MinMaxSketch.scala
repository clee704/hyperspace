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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Min}
import org.apache.spark.sql.types.DataType

import com.microsoft.hyperspace.index.dataskipping.util._

/**
 * Sketch based on minimum and maximum values for a given expression.
 */
case class MinMaxSketch(override val expr: String, override val dataType: Option[DataType] = None)
    extends SingleExprSketch[MinMaxSketch](expr, dataType) {
  override def name: String = "MinMax"

  override def withNewExpression(newExpr: (String, Option[DataType])): MinMaxSketch = {
    copy(expr = newExpr._1, dataType = newExpr._2)
  }

  override def aggregateFunctions: Seq[Expression] =
    Min(parsedExpr).toAggregateExpression() :: Max(parsedExpr).toAggregateExpression() :: Nil

  override def convertPredicate(
      predicate: Expression,
      sketchValues: Seq[Expression],
      nameMap: Map[ExprId, String],
      resolvedExprs: Seq[Expression]): Option[Expression] = {
    val min = sketchValues(0)
    val max = sketchValues(1)
    val exprMatcher = ExprMatcher(resolvedExprs.head, nameMap)
    val CEqualTo = ColumnEqualToValue(exprMatcher)
    val CLessThan = ColumnLessThanValue(exprMatcher)
    val CLessThanOrEqual = ColumnLessThanOrEqualToValue(exprMatcher)
    val CGreaterThan = ColumnGreaterThanValue(exprMatcher)
    val CGreaterThanOrEqual = ColumnGreaterThanOrEqualToValue(exprMatcher)
    val CIn = ColumnInValues(exprMatcher)
    val CIsNotNull = ColumnIsNotNull(exprMatcher)
    Option(predicate)
      .collect {
        case CEqualTo(value) => And(LessThanOrEqual(min, value), GreaterThanOrEqual(max, value))
        case CLessThan(value) => LessThan(min, value)
        case CLessThanOrEqual(value) => LessThanOrEqual(min, value)
        case CGreaterThan(value) => GreaterThan(max, value)
        case CGreaterThanOrEqual(value) => GreaterThanOrEqual(max, value)
        case CIn(values, dataType) =>
          values
            .map(value => And(LessThanOrEqual(min, value), GreaterThanOrEqual(max, value)))
            .reduceLeft(Or)
        case CIsNotNull() => Literal(true)
      }
      .map(p => And(IsNotNull(min), p))
  }
}
