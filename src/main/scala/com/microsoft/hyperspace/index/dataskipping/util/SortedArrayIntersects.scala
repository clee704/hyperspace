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
import org.apache.spark.sql.types.{ArrayType, BooleanType}

/**
 * Returns true if the left sorted array contains any of the values in the right sorted array.
 *
 * If either array is empty, false is returned.
 *
 * Both arrays must not be null.
 * Elements in the arrays must be in ascending order.
 * The left array should not contain duplicate elements.
 * The arrays must not contain null elements.
 */
case class SortedArrayIntersects(left: Expression, right: Expression) extends BinaryExpression {

  override def prettyName: String = "sorted_array_intersects"

  override def dataType: BooleanType = BooleanType

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(right.dataType.asInstanceOf[ArrayType].elementType)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Boolean = {
    val arr1 = left.eval(input).asInstanceOf[ArrayData]
    val arr2 = right.eval(input).asInstanceOf[ArrayData]
    val dt = right.dataType.asInstanceOf[ArrayType].elementType
    val n = arr1.numElements()
    val m = arr2.numElements()
    if (n > 0 && m > 0 &&
      ordering.lteq(arr1.get(0, dt), arr2.get(m - 1, dt)) &&
      ordering.lteq(arr2.get(0, dt), arr1.get(n - 1, dt))) {
      var i = 0
      var j = 0
      do {
        val v = arr1.get(i, dt)
        while (j < m && ordering.lt(arr2.get(j, dt), v)) j += 1
        if (j == m) return false
        val u = arr2.get(j, dt)
        j += 1
        val (found, k) = SortedArrayUtils.binarySearch(arr1, dt, ordering, i, n, u)
        if (found) return true
        if (k == n) return false
        i = k
      } while (j < m)
    }
    false
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx)
    val arr1 = leftGen.value
    val rightGen = right.genCode(ctx)
    val arr2 = rightGen.value
    val dt = right.dataType.asInstanceOf[ArrayType].elementType
    val javaType = CodeGenerator.javaType(dt)
    val n = ctx.freshName("n")
    val m = ctx.freshName("m")
    val i = ctx.freshName("i")
    val j = ctx.freshName("j")
    val v = ctx.freshName("v")
    val u = ctx.freshName("u")
    val result = ctx.freshName("result")
    val binarySearchResultType =
      SortedArrayUtils.BinarySearchResult.getClass.getCanonicalName.stripSuffix("$")
    val binarySearch = SortedArrayUtils.binarySearchCodeGen(ctx, dt)
    import CodeGenerator.getValue
    val resultCode =
      s"""
         |int $n = $arr1.numElements();
         |int $m = $arr2.numElements();
         |if ($n > 0 && $m > 0 &&
         |    !(${ctx.genGreater(dt, getValue(arr1, dt, "0"), getValue(arr2, dt, s"$m - 1"))}) &&
         |    !(${ctx.genGreater(dt, getValue(arr2, dt, "0"), getValue(arr1, dt, s"$n - 1"))})) {
         |  int $i = 0;
         |  int $j = 0;
         |  do {
         |    $javaType $v = ${getValue(arr1, dt, i)};
         |    while ($j < $m && ${ctx.genGreater(dt, v, getValue(arr2, dt, j))}) $j += 1;
         |    if ($j == $m) break;
         |    $javaType $u = ${getValue(arr2, dt, j)};
         |    $j += 1;
         |    $binarySearchResultType $result = $binarySearch($arr1, $i, $n, $u);
         |    if ($result.found()) {
         |      ${ev.value} = true;
         |      break;
         |    }
         |    if ($result.index() == $n) break;
         |    $i = $result.index();
         |  } while ($j < $m);
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
