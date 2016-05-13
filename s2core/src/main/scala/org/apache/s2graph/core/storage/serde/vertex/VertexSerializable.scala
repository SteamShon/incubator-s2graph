/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.storage.serde.vertex

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.Vertex
import org.apache.s2graph.core.storage.{SKeyValue, Serializable}

case class VertexSerializable(vertex: Vertex) extends Serializable[Vertex] {

  val cf = Serializable.vertexCf

  override def toKeyValues: Seq[SKeyValue] = {
    val row = vertex.vertexId.bytes
    val base = for ((k, v) <- vertex.props ++ vertex.defaultProps) yield Bytes.toBytes(k) -> v.bytes
    val belongsTo = vertex.belongLabelIds.map { labelId => Bytes.toBytes(Vertex.toPropKey(labelId)) -> Array.empty[Byte] }
    (base ++ belongsTo).map { case (qualifier, value) =>
      SKeyValue(vertex.hbaseTableName.getBytes, row, cf, qualifier, value, vertex.ts)
    } toSeq
  }
}
