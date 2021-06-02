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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{BooleanType, DataType}

case class ColumnEqualToValue(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[Literal] =
    p match {
      case EqualTo(c, v: Literal) if v.value != null && matcher(c) => Some(v)
      case EqualTo(v: Literal, c) if v.value != null && matcher(c) => Some(v)
      case _ => None
    }
}

case class ColumnLessThanValue(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[Literal] =
    p match {
      case LessThan(c, v: Literal) if v.value != null && matcher(c) => Some(v)
      case GreaterThan(v: Literal, c) if v.value != null && matcher(c) => Some(v)
      case _ => None
    }
}

case class ColumnLessThanOrEqualToValue(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[Literal] =
    p match {
      case LessThanOrEqual(c, v: Literal) if v.value != null && matcher(c) => Some(v)
      case GreaterThanOrEqual(v: Literal, c) if v.value != null && matcher(c) => Some(v)
      case _ => None
    }
}

case class ColumnGreaterThanValue(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[Literal] =
    p match {
      case GreaterThan(c, v: Literal) if v.value != null && matcher(c) => Some(v)
      case LessThan(v: Literal, c) if v.value != null && matcher(c) => Some(v)
      case _ => None
    }
}

case class ColumnGreaterThanOrEqualToValue(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[Literal] =
    p match {
      case GreaterThanOrEqual(c, v: Literal) if v.value != null && matcher(c) => Some(v)
      case LessThanOrEqual(v: Literal, c) if v.value != null && matcher(c) => Some(v)
      case _ => None
    }
}

case class ColumnIsNotNull(matcher: ExprMatcher) {
  def unapply(p: Expression): Boolean =
    p match {
      case IsNotNull(c) if matcher(c) => true
      case _ => false
    }
}

case class ColumnInValues(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[(Seq[Expression], DataType)] =
    p match {
      case In(c, vs)
          if vs.nonEmpty && vs.forall(v =>
            v.isInstanceOf[Literal] && v.asInstanceOf[Literal].value != null) && matcher(c) =>
        Some((vs, c.dataType))
      case _ => None
    }
}

case class ColumnEqualToTrue(matcher: ExprMatcher) {
  def unapply(p: Expression): Boolean = p.dataType == BooleanType && matcher(p)
}

case class ExprMatcher(expr: Expression, nameMap: Map[ExprId, String]) {
  def apply(c: Expression): Boolean = {
    val normalized = ExpressionUtils.normalize(c.transformUp {
      case a: AttributeReference => a.withName(nameMap.getOrElse(a.exprId, return false))
    })
    expr == normalized
  }
}
