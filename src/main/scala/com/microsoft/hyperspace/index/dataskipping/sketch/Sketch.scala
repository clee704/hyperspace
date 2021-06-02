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

import org.apache.spark.sql.Column

/**
 * Represents a sketch specification for data skipping indexes.
 */
trait Sketch {

  /**
   * Returns column names that this sketch is based on.
   */
  def columns: Seq[String]

  /**
   * Returns a copy of this sketch with updated columns.
   */
  def withNewColumns(newColumns: Seq[String]): Sketch

  /**
   * Returns aggregate functions that can be used to compute the actual sketch
   * values from source data.
   *
   * It should return a non-empty sequence and the length of the sequence
   * should be the same as [[numValues]].
   */
  def aggregateFunctions: Seq[Column]

  /**
   * Returns the number of the values this sketch creates per file.
   */
  def numValues: Int

  /**
   * Returns a human-readable string describing this sketch.
   */
  def toString: String
}
