package org.apache.s2graph.core.tinkerpop.structure

import java.util

import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure._

class S2Vertex(val graph: S2Graph,
               val id: AnyRef,
               val serviceName: String,
               val columnName: String,
               val label: String) extends Vertex {
  override def vertices(direction: Direction, strings: String*): util.Iterator[Vertex] = ???

  override def edges(direction: Direction, strings: String*): util.Iterator[Edge] = ???

  override def property[V](cardinality: Cardinality, s: String, v: V, objects: AnyRef*): VertexProperty[V] = ???

  override def addEdge(s: String, vertex: Vertex, objects: AnyRef*): Edge = ???

  override def properties[V](strings: String*): util.Iterator[VertexProperty[V]] = ???

  override def remove(): Unit = ???

}
