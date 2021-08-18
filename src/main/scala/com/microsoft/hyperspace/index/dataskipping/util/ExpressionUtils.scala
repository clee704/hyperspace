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

import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project, Window}
import org.apache.spark.sql.types.DataType

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.IndexUtils
import com.microsoft.hyperspace.index.dataskipping.sketch.Sketch
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.withHyperspaceRuleDisabled

object ExpressionUtils {

  val nullExprId = ExprId(0, new UUID(0, 0))

  /**
   * Returns copies of the given sketches with the indexed columns replaced by
   * resolved column names and data types.
   */
  def resolve(spark: SparkSession, sketches: Seq[Sketch], sourceData: DataFrame): Seq[Sketch] = {
    sketches.map { s =>
      val dataTypes = checkExprs(s.expressions, sourceData)
      val oldColumns = s.referencedColumns
      val newColumns = IndexUtils.resolveColumns(sourceData, oldColumns).map(_.name)
      val columnMapping = oldColumns.zip(newColumns).toMap
      val newExprs = s.expressions.map {
        case (expr, _) =>
          spark.sessionState.sqlParser
            .parseExpression(expr)
            .transformUp {
              case attr: UnresolvedAttribute => QuotedAttribute(columnMapping(attr.name))
            }
            .sql
      }
      s.withNewExpressions(newExprs.zip(dataTypes.map(Some(_))))
    }
  }

  private def checkExprs(
      exprWithExpectedDataTypes: Seq[(String, Option[DataType])],
      sourceData: DataFrame): Seq[DataType] = {
    val (exprs, expectedDataTypes) =
      (exprWithExpectedDataTypes.map(_._1), exprWithExpectedDataTypes.map(_._2))
    def throwNotSupportedIf(cond: Boolean, msg: => String) = {
      if (cond) {
        throw HyperspaceException(s"DataSkippingIndex does not support indexing $msg")
      }
    }
    val plan = sourceData.selectExpr(exprs: _*).queryExecution.analyzed
    throwNotSupportedIf(
      plan.isInstanceOf[Aggregate],
      "aggregate functions: " + exprs.mkString(", "))
    throwNotSupportedIf(
      plan.find(_.isInstanceOf[Window]).nonEmpty,
      "window functions: " + exprs.mkString(", "))
    val analyzedExprs = plan.asInstanceOf[Project].projectList
    exprWithExpectedDataTypes.zip(analyzedExprs).map {
      case ((expr, expectedDataType), analyzedExpr) =>
        val e = analyzedExpr match {
          case Alias(child, _) => child
          case e => e
        }
        throwNotSupportedIf(!e.deterministic, s"an expression which is non-deterministic: $expr")
        throwNotSupportedIf(e.foldable, s"an expression which is evaluated to a constant: $expr")
        throwNotSupportedIf(
          e.find(_.isInstanceOf[SubqueryExpression]).nonEmpty,
          s"an expression which has a subquery: $expr")
        throwNotSupportedIf(
          e.find(_.isInstanceOf[AttributeReference]).isEmpty,
          s"an expression which does not reference source columns: $expr")
        if (expectedDataType.nonEmpty && expectedDataType.get != analyzedExpr.dataType) {
          throw HyperspaceException(
            "Specified and analyzed data types differ: " +
              s"expr=$expr, specified=${expectedDataType.get}, analyzed=${analyzedExpr.dataType}")
        }
        analyzedExpr.dataType
    }
  }

  /**
   * Used to workaround the issue where UnresolvedAttribute.sql() doesn't work as expected.
   */
  private case class QuotedAttribute(name: String) extends LeafExpression {
    override def sql: String = name

    // $COVERAGE-OFF$ code never used
    override def nullable: Boolean = throw new NotImplementedError
    override def eval(input: InternalRow): Any = throw new NotImplementedError
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      throw new NotImplementedError
    override def dataType: DataType = throw new NotImplementedError
    // $COVERAGE-ON$
  }

  /**
   * Returns a normalized expression so that the index expression and an
   * expression in the predicate can be matched.
   */
  def normalize(expr: Expression): Expression = {
    expr.transformUp {
      case a: AttributeReference => a.withExprId(nullExprId)
      case g @ GetStructField(child, ordinal, _) => g.copy(child, ordinal, None)
      // Undo HandleNullInputsForUDF
      case If(
            ExtractIsNullDisjunction(args1),
            // ReplaceNullWithFalseInPredicate can change null to false
            Literal(null | false, dataType1),
            udf @ ExtractScalaUDF(dataType2, ExtractKnownNotNullArgs(args2)))
          if args1 == args2 && dataType1 == dataType2 =>
        udf.copy(children = args2)
    }
  }

  // Needed because ScalaUDF has a different number of arguments depending on Spark versions.
  private object ExtractScalaUDF {
    def unapply(e: ScalaUDF): Option[(DataType, Seq[Expression])] = {
      Some((e.dataType, e.children))
    }
  }

  private object ExtractIsNullDisjunction {
    def unapply(pred: Expression): Option[Seq[Expression]] =
      pred match {
        case IsNull(arg) => Some(Seq(arg))
        case Or(IsNull(arg), ExtractIsNullDisjunction(args)) => Some(arg +: args)
        case _ => None
      }
  }

  private object ExtractKnownNotNullArgs {
    def unapply(args: Seq[Expression]): Option[Seq[Expression]] = {
      if (args.forall(_.isInstanceOf[KnownNotNull])) {
        Some(args.map(_.asInstanceOf[KnownNotNull].child))
      } else {
        None
      }
    }
  }

  def getResolvedExprs(
      spark: SparkSession,
      sketches: Seq[Sketch],
      source: LogicalPlan): Option[Map[Sketch, Seq[Expression]]] = {
    val resolvedExprs = sketches.map { s =>
      val parsedExprs = s.expressions.map {
        case (expr, _) => PredicateWrapper(spark.sessionState.sqlParser.parseExpression(expr))
      }
      val cond = parsedExprs.reduceLeft(And)
      val filter = withHyperspaceRuleDisabled {
        spark.sessionState.optimizer
          .execute(spark.sessionState.analyzer.execute(Filter(cond, source)))
          .asInstanceOf[Filter]
      }
      val resolved = filter.condition.collect {
        case PredicateWrapper(expr) => normalize(expr)
      }
      s.expressions.map(_._2.get).zip(resolved).foreach {
        case (dataType, resolvedExpr) =>
          if (dataType != resolvedExpr.dataType) {
            return None
          }
      }
      s -> resolved
    }.toMap
    Some(resolvedExprs)
  }

  // Used to preserve sketch expressions during optimization
  private case class PredicateWrapper(override val child: Expression)
      extends UnaryExpression
      with Predicate {
    // $COVERAGE-OFF$ code never used
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      throw new NotImplementedError
    // $COVERAGE-ON$
  }
}
