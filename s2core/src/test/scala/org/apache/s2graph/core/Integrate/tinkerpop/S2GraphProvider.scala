package org.apache.s2graph.core.Integrate.tinkerpop

import java.util

import org.apache.commons.configuration.Configuration
import org.apache.tinkerpop.gremlin.AbstractGraphProvider
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData
import org.apache.tinkerpop.gremlin.structure.Graph

class S2GraphProvider extends AbstractGraphProvider {
  override def getBaseConfiguration(s: String, aClass: Class[_], s1: String, graphData: GraphData): util.Map[String, AnyRef] = ???

  override def clear(graph: Graph, configuration: Configuration): Unit = ???

  override def getImplementations: util.Set[Class[_]] = ???
}
