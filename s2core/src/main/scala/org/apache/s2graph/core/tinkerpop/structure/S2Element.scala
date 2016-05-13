package org.apache.s2graph.core.tinkerpop.structure

import java.util

import org.apache.tinkerpop.gremlin.structure.{Property, Graph, Element}

class S2Element extends Element {
  override def property[V](s: String, v: V): Property[V] = ???

  override def remove(): Unit = ???

  override def properties[V](strings: String*): util.Iterator[_ <: Property[V]] = ???

  override def graph(): Graph = ???

  override def label(): String = ???

  override def id(): AnyRef = ???
}
