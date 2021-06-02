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
import org.apache.spark.sql.types._

import com.microsoft.hyperspace.index.HyperspaceSuite
import com.microsoft.hyperspace.index.dataskipping.ArrayTestUtils

class SortedArrayContainsTest extends HyperspaceSuite with ArrayTestUtils {
  test("SortedArrayContains works correctly for an int array.") {
    val values = Seq.range(0, 100000).map(_ * 2)
    val array = createArray(values, IntegerType)
    values.foreach(v =>
      assert(SortedArrayContains(array, Literal(v, IntegerType)).eval() === true))
    assert(SortedArrayContains(array, Literal(-10, IntegerType)).eval() === false)
    assert(SortedArrayContains(array, Literal(1, IntegerType)).eval() === false)
    assert(SortedArrayContains(array, Literal(999, IntegerType)).eval() === false)
    assert(SortedArrayContains(array, Literal(1000000000, IntegerType)).eval() === false)
  }

  test("SortedArrayContains works correctly for a long array.") {
    val values = Seq.range(0L, 100000L).map(_ * 2)
    val array = createArray(values, LongType)
    values.foreach(v => assert(SortedArrayContains(array, Literal(v, LongType)).eval() === true))
    assert(SortedArrayContains(array, Literal(-10L, LongType)).eval() === false)
    assert(SortedArrayContains(array, Literal(1L, LongType)).eval() === false)
    assert(SortedArrayContains(array, Literal(999L, LongType)).eval() === false)
    assert(SortedArrayContains(array, Literal(1000000000L, LongType)).eval() === false)
  }

  test("SortedArrayContains works correctly for a string array.") {
    val values = Seq("hello", "world", "foo", "bar", "footrix").sorted
    val array = createArray(values, StringType)
    values.foreach(v =>
      assert(SortedArrayContains(array, Literal.create(v, StringType)).eval() === true))
    assert(SortedArrayContains(array, Literal.create("abc", StringType)).eval() === false)
    assert(SortedArrayContains(array, Literal.create("fooo", StringType)).eval() === false)
    assert(SortedArrayContains(array, Literal.create("zoo", StringType)).eval() === false)
  }
}
