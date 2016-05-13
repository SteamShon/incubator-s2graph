package org.apache.s2graph.core.tinkerpop.structure

import java.util

import org.apache.tinkerpop.gremlin.structure._

class S2Edge extends Edge {
  override def vertices(direction: Direction): util.Iterator[Vertex] = ???

  override def properties[V](strings: String*): util.Iterator[Property[V]] = ???

  override def property[V](s: String, v: V): Property[V] = ???

  override def remove(): Unit = ???

  override def graph(): Graph = ???

  override def label(): String = ???

  override def id(): AnyRef = ???
}
