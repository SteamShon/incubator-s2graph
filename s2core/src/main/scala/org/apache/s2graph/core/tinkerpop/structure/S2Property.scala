package org.apache.s2graph.core.tinkerpop.structure

import org.apache.tinkerpop.gremlin.structure.util.{ElementHelper, StringFactory}
import org.apache.tinkerpop.gremlin.structure.{Edge, Element, Property}

class S2Property[V](val element: Element,
                    val key: String,
                    val value: V) extends Property[V] {

  override def isPresent: Boolean = null != this.value

  override def remove(): Unit =
    element match {
//      case edge: Edge =>
      case _ => throw new RuntimeException("not supported vertex yet.")
//        element.asInstanceOf[S2VertexProperty]
    }
  override def toString(): String = StringFactory.propertyString(this)

  override def equals(other: Any): Boolean = ElementHelper.areEqual(this, other)

  override def hashCode(): Int = ElementHelper.hashCode(this)

}
