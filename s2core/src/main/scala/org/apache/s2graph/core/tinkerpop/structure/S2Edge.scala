package org.apache.s2graph.core.tinkerpop.structure

import java.util

import org.apache.s2graph._
import org.apache.tinkerpop.gremlin.structure._
import org.apache.tinkerpop.gremlin.structure.util.StringFactory
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils

class S2Edge(val s2Graph: S2Graph, val coreEdge: core.Edge)
  extends Edge(coreEdge.id,
    s2Graph.toS2Vertex(coreEdge.srcVertex),
    coreEdge.label.label,
    s2Graph.toS2Vertex(coreEdge.tgtVertex)) {

  override def vertices(direction: Direction): util.Iterator[Vertex] = {
    direction match {
      case Direction.OUT => IteratorUtils.of(outVertex)
      case Direction.IN => IteratorUtils.of(inVertex)
      case _ => IteratorUtils.of(outVertex, inVertex)
    }
  }

  override def properties[V](strings: String*): util.Iterator[Property[V]] = ???

  override def property[V](s: String, v: V): Property[V] = ???

  override def remove(): Unit = ???

  override def graph(): Graph = inVertex.graph()

  override def toString(): String = StringFactory.edgeString(this)

  override def label(): String = super.label()

  override def id(): AnyRef = super.id()
}
