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

class SortedArrayLowerBoundTest extends HyperspaceSuite with ArrayTestUtils {
  def eval(dataType: DataType, arr: Seq[Any], value: Any): Any = {
    SortedArrayLowerBound(createArray(arr, dataType), Literal.create(value, dataType)).eval()
  }

  test("SortedArrayLowerBound returns null if the array is empty.") {
    assert(eval(IntegerType, Nil, 0) === null)
  }

  test("SortedArrayLowerBound returns the index if the value is in the array.") {
    assert(eval(IntegerType, Seq(1), 1) === 1)
    assert(eval(IntegerType, Seq(2), 2) === 1)
    assert(eval(IntegerType, Seq(1, 3), 1) === 1)
    assert(eval(IntegerType, Seq(1, 3), 3) === 2)
    assert(eval(IntegerType, Seq(1, 3, 5), 1) === 1)
    assert(eval(IntegerType, Seq(1, 3, 5), 3) === 2)
    assert(eval(IntegerType, Seq(1, 3, 5), 5) === 3)
    assert(eval(DoubleType, Seq(1.5, 3.0, 4.5), 1.5) === 1)
    assert(eval(DoubleType, Seq(1.5, 3.0, 4.5), 3.0) === 2)
    assert(eval(DoubleType, Seq(1.5, 3.0, 4.5), 4.5) === 3)
    assert(eval(StringType, Seq("foo"), "foo") === 1)
    assert(eval(StringType, Seq("bar", "foo"), "bar") === 1)
    assert(eval(StringType, Seq("bar", "foo"), "foo") === 2)
  }

  test(
    "SortedArrayLowerBound returns the index if the first value in the array " +
      "which is not less than the value.") {
    assert(eval(IntegerType, Seq(1), 0) === 1)
    assert(eval(IntegerType, Seq(1, 3), 0) === 1)
    assert(eval(IntegerType, Seq(1, 3), 2) === 2)
    assert(eval(IntegerType, Seq(1, 3, 5), 0) === 1)
    assert(eval(IntegerType, Seq(1, 3, 5), 2) === 2)
    assert(eval(IntegerType, Seq(1, 3, 5), 4) === 3)
  }

  test(
    "SortedArrayLowerBound returns null if the value is greater than " +
      "the last value in the array.") {
    assert(eval(IntegerType, Seq(1), 2) === null)
    assert(eval(IntegerType, Seq(1, 3), 4) === null)
    assert(eval(IntegerType, Seq(1, 3, 5), 6) === null)
  }
}
