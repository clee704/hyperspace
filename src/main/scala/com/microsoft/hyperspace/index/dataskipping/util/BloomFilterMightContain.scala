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
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.util.sketch.BloomFilter

/**
 * Returns true if the bloom filter (left) might contain the value (right).
 *
 * The bloom filter and the value must not be null.
 */
case class BloomFilterMightContain(left: Expression, right: Expression) extends BinaryExpression {

  override def prettyName: String = "bloom_filter_might_contain"

  override def dataType: BooleanType = BooleanType

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Boolean = {
    val bfData = left.eval(input)
    val bf = BloomFilterEncoderProvider.defaultEncoder.decode(bfData)
    val value = right.eval(input)
    BloomFilterUtils.mightContain(bf, value, right.dataType)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val bloomFilterEncoder =
      BloomFilterEncoderProvider.defaultEncoder.getClass.getCanonicalName.stripSuffix("$")
    val bf = s"$bloomFilterEncoder.decode(${leftGen.value})"
    val result = s"${BloomFilterUtils.mightContainCodegen(bf, rightGen.value, right.dataType)}"
    ev.copy(
      code = code"""
        ${leftGen.code}
        ${rightGen.code}
        boolean ${ev.value} = $result;""",
      isNull = FalseLiteral)
  }
}

/**
 * Returns true if the bloom filter (left) might contain one of the values (right).
 *
 * The bloom filter must not be null.
 * There must be at least one value. The values must not be null.
 */
case class BloomFilterMightContainAny(left: Expression, right: Seq[Expression])
    extends Expression {
  assert(right.nonEmpty)

  override def children: Seq[Expression] = left +: right

  override def prettyName: String = "bloom_filter_might_contain_any"

  override def dataType: BooleanType = BooleanType

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Boolean = {
    val bfData = left.eval(input)
    val bf = BloomFilterEncoderProvider.defaultEncoder.decode(bfData)
    val values = right.map(_.eval(input))
    val dt = right.head.dataType
    values.exists(BloomFilterUtils.mightContain(bf, _, dt))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx);
    val rightGens = right.map(_.genCode(ctx))
    val bloomFilterEncoder =
      BloomFilterEncoderProvider.defaultEncoder.getClass.getCanonicalName.stripSuffix("$")
    val bf = ctx.freshName("bf")
    val values = ctx.freshName("values")
    val i = ctx.freshName("i")
    val bfType = classOf[BloomFilter].getCanonicalName
    val dt = right.head.dataType
    val javaType = CodeGenerator.javaType(dt)
    val resultCode =
      s"""
         |$bfType $bf = $bloomFilterEncoder.decode(${leftGen.value});
         |$javaType[] $values = new $javaType[] {${rightGens.map(_.value).mkString(", ")}};
         |for (int $i = 0; $i < $values.length; $i++) {
         |  if (${BloomFilterUtils.mightContainCodegen(bf, s"$values[$i]", dt)}) {
         |    ${ev.value} = true;
         |    break;
         |  }
         |}
       """.stripMargin
    ev.copy(
      code = code"""
        ${leftGen.code}
        ${rightGens.map(_.code).mkString}
        boolean ${ev.value} = false;
        $resultCode""",
      isNull = FalseLiteral)
  }
}
