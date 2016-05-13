package org.apache.s2graph.core.tinkerpop.process

import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper

object S2GraphStepStrategy {
  def instance(): S2GraphStepStrategy = {
    new S2GraphStepStrategy()
  }
}
class S2GraphStepStrategy extends TraversalStrategy[TraversalStrategy.ProviderOptimizationStrategy] {
  override def apply(traversal: Admin[_, _]): Unit = {
    if (!TraversalHelper.onGraphComputer(traversal)) {
      for {
        originalGraphStep <- TraversalHelper.getStepsOfClass(classOf[GraphStep], traversal)
      } {

      }

//      .forEach(originalGraphStep -> {
//        final TinkerGraphStep<?, ?> tinkerGraphStep = new TinkerGraphStep<>(originalGraphStep);
//        TraversalHelper.replaceStep(originalGraphStep, (Step) tinkerGraphStep, traversal);
//        Step<?, ?> currentStep = tinkerGraphStep.getNextStep();
//        while (currentStep instanceof HasContainerHolder) {
//          ((HasContainerHolder) currentStep).getHasContainers().forEach(hasContainer -> {
//            if (!GraphStep.processHasContainerIds(tinkerGraphStep, hasContainer))
//              tinkerGraphStep.addHasContainer(hasContainer);
//          });
//          currentStep.getLabels().forEach(tinkerGraphStep::addLabel);
//          traversal.removeStep(currentStep);
//          currentStep = currentStep.getNextStep();
//        }
//      });
    }
  }
}
