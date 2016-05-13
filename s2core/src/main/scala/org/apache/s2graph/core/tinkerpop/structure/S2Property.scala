package org.apache.s2graph.core.tinkerpop.structure

import org.apache.tinkerpop.gremlin.structure.{Element, Property}

class S2Property[V] extends Property {
  override def isPresent: Boolean = ???

  override def key(): String = ???

  override def value(): V = ???

  override def remove(): Unit = ???

  override def element(): Element = ???
}
