package org.apache.s2graph.core.tinkerpop.structure

import java.util

import org.apache.commons.configuration.Configuration
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.structure.Graph.Variables
import org.apache.tinkerpop.gremlin.structure.{Transaction, Edge, Vertex, Graph}

class S2Graph extends Graph {
  override def vertices(objects: AnyRef*): util.Iterator[Vertex] = ???

  override def tx(): Transaction = ???

  override def edges(objects: AnyRef*): util.Iterator[Edge] = ???

  override def variables(): Variables = ???

  override def configuration(): Configuration = ???

  override def addVertex(objects: AnyRef*): Vertex = ???

  override def close(): Unit = ???

  override def compute[C <: GraphComputer](aClass: Class[C]): C = ???

  override def compute(): GraphComputer = ???
}
