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
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{ArrayType, DataType}

import com.microsoft.hyperspace.index.dataskipping.util._

/**
 * Sketch based on distinct values for a given expression.
 *
 * This is not really a sketch, as it stores all distinct values for a given
 * expression. It can be useful when the number of distinct values is expected to
 * be small and each file tends to store only a subset of the values.
 */
case class ValueListSketch(
    override val expr: String,
    override val dataType: Option[DataType] = None)
    extends SingleExprSketch[ValueListSketch](expr, dataType) {
  override def name: String = "ValueList"

  override def withNewExpression(newExpr: (String, Option[DataType])): ValueListSketch = {
    copy(expr = newExpr._1, dataType = newExpr._2)
  }

  override def aggregateFunctions: Seq[Expression] =
    new ArraySort(CollectSet(parsedExpr).toAggregateExpression()) :: Nil

  override def convertPredicate(
      predicate: Expression,
      sketchValues: Seq[Expression],
      nameMap: Map[ExprId, String],
      resolvedExprs: Seq[Expression]): Option[Expression] = {
    val valueList = sketchValues(0)
    val min = ElementAt(valueList, Literal(1))
    val max = ElementAt(valueList, Literal(-1))
    val exprMatcher = ExprMatcher(resolvedExprs.head, nameMap)
    val CEqualTo = ColumnEqualToValue(exprMatcher)
    val CLessThan = ColumnLessThanValue(exprMatcher)
    val CLessThanOrEqual = ColumnLessThanOrEqualToValue(exprMatcher)
    val CGreaterThan = ColumnGreaterThanValue(exprMatcher)
    val CGreaterThanOrEqual = ColumnGreaterThanOrEqualToValue(exprMatcher)
    val CIn = ColumnInValues(exprMatcher)
    val CIsNotNull = ColumnIsNotNull(exprMatcher)
    val CEqualToTrue = ColumnEqualToTrue(exprMatcher)
    Option(predicate).collect {
      case CEqualTo(value) => SortedArrayContains(valueList, value)
      case Not(CEqualTo(value)) =>
        And(
          Not(EqualTo(Size(valueList), Literal(0))),
          Or(
            Not(EqualTo(Size(valueList), Literal(1))),
            Not(EqualTo(ElementAt(valueList, Literal(1)), value))))
      case CLessThan(value) => LessThan(min, value)
      case CLessThanOrEqual(value) => LessThanOrEqual(min, value)
      case CGreaterThan(value) => GreaterThan(max, value)
      case CGreaterThanOrEqual(value) => GreaterThanOrEqual(max, value)
      case CIn(values, dataType) =>
        implicit val ordering = TypeUtils.getInterpretedOrdering(dataType)
        val valuesArray =
          Literal.create(values.map(_.eval()).sorted, ArrayType(dataType, containsNull = false))
        SortedArrayIntersects(valueList, valuesArray)
      case CIsNotNull() => Not(EqualTo(Size(valueList), Literal(0)))
      case CEqualToTrue() => ArrayContains(valueList, Literal(true))
      // TODO: Like
    }
  }
}
