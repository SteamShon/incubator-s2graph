package org.apache.s2graph.core.tinkerpop.process

import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy

class S2GraphStepStrategy extends TraversalStrategy {
  override def apply(admin: Admin[_, _]): Unit = ???
}
