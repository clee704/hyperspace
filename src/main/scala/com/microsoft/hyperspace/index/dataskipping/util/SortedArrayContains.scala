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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayData, TypeUtils}
import org.apache.spark.sql.types.BooleanType

/**
 * Returns true if the sorted array (left) might contain the value (right).
 *
 * The array must not be null.
 * Elements in the array must be in ascending order.
 * The array must not contain null elements.
 * The array must not contain duplicate elements.
 * The value must not be null.
 */
case class SortedArrayContains(left: Expression, right: Expression) extends BinaryExpression {

  override def prettyName: String = "sorted_array_contains"

  override def dataType: BooleanType = BooleanType

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(right.dataType)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Boolean = {
    val arr = left.eval(input).asInstanceOf[ArrayData]
    val value = right.eval(input)
    val dt = right.dataType
    val n = arr.numElements()
    if (n > 0 &&
      ordering.lteq(arr.get(0, dt), value) &&
      ordering.lteq(value, arr.get(n - 1, dt))) {
      val (found, _) = SortedArrayUtils.binarySearch(arr, dt, ordering, 0, n, value)
      if (found) return true
    }
    false
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx)
    val arr = leftGen.value
    val rightGen = right.genCode(ctx)
    val value = rightGen.value
    val dt = right.dataType
    val n = ctx.freshName("n")
    val binarySearch = SortedArrayUtils.binarySearchCodeGen(ctx, dt)
    val resultCode =
      s"""
         |int $n = $arr.numElements();
         |if ($n > 0 &&
         |    !(${ctx.genGreater(dt, CodeGenerator.getValue(arr, dt, "0"), value)}) &&
         |    !(${ctx.genGreater(dt, value, CodeGenerator.getValue(arr, dt, s"$n - 1"))})) {
         |  ${ev.value} = $binarySearch($arr, 0, $n, $value).found();
         |}
       """.stripMargin
    ev.copy(
      code = code"""
        ${leftGen.code}
        ${rightGen.code}
        boolean ${ev.value} = false;
        $resultCode""",
      isNull = FalseLiteral)
  }
}
