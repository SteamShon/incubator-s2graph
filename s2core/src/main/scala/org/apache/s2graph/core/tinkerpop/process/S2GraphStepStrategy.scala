package org.apache.s2graph.core.tinkerpop.process


import org.apache.s2graph.core.tinkerpop.process.traversal.step.sideEffect.S2GraphStep
import org.apache.s2graph.core.tinkerpop.structure.{S2Graph, S2Element}
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.process.traversal.Step
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import scala.collection.JavaConversions._

object S2GraphStepStrategy {
  def instance(): S2GraphStepStrategy = {
    new S2GraphStepStrategy()
  }
}
class S2GraphStepStrategy extends AbstractTraversalStrategy[ProviderOptimizationStrategy] with ProviderOptimizationStrategy {
  override def apply(traversal: Admin[_, _]): Unit = {
    logger.debug(s"[S2GraphStepStrategy]")
    if (!TraversalHelper.onGraphComputer(traversal)) {
      //
    }

  }
}
