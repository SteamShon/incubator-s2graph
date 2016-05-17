package org.apache.s2graph.core.tinkerpop.structure

import java.util

import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
import org.apache.tinkerpop.gremlin.structure.{Element, Graph, Property}

abstract class S2Element(val id: AnyRef,
                         service: Option[String],
                         val label: String) extends Element {

  override def remove(): Unit = {
    // to nothing.
  }

  override def graph(): Graph = ???

  override def hashCode(): Int = ElementHelper.hashCode(this)

}
