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

package com.microsoft.hyperspace.index.types.covering

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.index.rules.ApplyHyperspace.PlanToIndexesMap
import com.microsoft.hyperspace.index.rules.QueryPlanIndexFilter

/**
 * Filters out indexes which are not [[CoveringIndex]].
 */
object CoveringIndexFilter extends QueryPlanIndexFilter {
  override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
    candidateIndexes
      .map {
        case (plan, indexes) =>
          plan -> indexes.filter(_.derivedDataset.isInstanceOf[CoveringIndex])
      }
      .filter { case (_, indexes) => indexes.nonEmpty }
  }
}
