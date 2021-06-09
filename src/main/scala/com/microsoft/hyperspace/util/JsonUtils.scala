/*
 * Copyright (2020) The Hyperspace Project Authors.
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

package com.microsoft.hyperspace.util

import scala.math.Ordered.orderingToOrdered

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{JsonDeserializer, JsonSerializer, ObjectMapper}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.hyperspace.module.scala.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.shim.{StructTypeDeserializer, StructTypeSerializer}

/**
 * Useful json functions used around the Hyperspace codebase.
 */
object JsonUtils {

  /** Used to convert between classes and JSON. */
  private val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.setSerializationInclusion(Include.ALWAYS)
  objectMapper.registerModule(DefaultScalaModule)

  if (BuildInfoExt.sparkVersion < (3, 0, 0)) {
    // Fix StructType serialization in Spark 2
    objectMapper.registerModule {
      new SimpleModule()
        .addSerializer(classOf[StructType], new StructTypeSerializer)
        .addDeserializer(classOf[StructType], new StructTypeDeserializer)
    }
  }

  def toJson[T: Manifest](obj: T): String = {
    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
  }

  def fromJson[T: Manifest](json: String): T = {
    objectMapper.readValue[T](json)
  }

  def jsonToMap(json: String): Map[String, Any] = {
    objectMapper.readValue[Map[String, Any]](json)
  }
}
