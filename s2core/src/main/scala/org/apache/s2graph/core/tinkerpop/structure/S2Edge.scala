package org.apache.s2graph.core.tinkerpop.structure

import java.util

import org.apache.s2graph._
import org.apache.tinkerpop.gremlin.structure._
import org.apache.tinkerpop.gremlin.structure.util.StringFactory
import scala.collection.JavaConversions._

class S2Edge(val s2Graph: S2Graph,
             val coreEdge: core.Edge) extends Edge {

  override val label = coreEdge.label.label
  override val outVertex = s2Graph.toS2Vertex(coreEdge.srcVertex)
  override val inVertex = s2Graph.toS2Vertex(coreEdge.tgtVertex)

  override def vertices(direction: Direction): util.Iterator[Vertex] = {
    val ls = direction match {
      case Direction.OUT => Seq(outVertex)
      case Direction.IN => Seq(inVertex)
      case _ => Seq(outVertex, inVertex)
    }
    ls.iterator
  }

  override def properties[V](strings: String*): util.Iterator[Property[V]] = ???

  override def property[V](s: String, v: V): Property[V] = ???

  override def remove(): Unit = ???

  override def graph(): Graph = s2Graph

  override def toString(): String = StringFactory.edgeString(this)

  override def id(): AnyRef = {
    coreEdge.id
  }
}
