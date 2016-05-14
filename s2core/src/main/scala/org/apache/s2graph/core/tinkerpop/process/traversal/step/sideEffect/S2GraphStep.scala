package org.apache.s2graph.core.tinkerpop.process.traversal.step.sideEffect

import java.util
import java.util.function.Supplier

import org.apache.s2graph.core.tinkerpop.structure.S2Graph
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.apache.tinkerpop.gremlin.structure.{Edge, Graph, Vertex, Element}
import scala.collection.JavaConversions._

class S2GraphStep[S, E <: Element](orgStep: GraphStep[S, E])
  extends GraphStep[S, E](orgStep.getTraversal(),
    orgStep.getReturnClass(),
    orgStep.isStartStep(),
    orgStep.getIds()) with HasContainerHolder {

  /** init */
  orgStep.getLabels.foreach(addLabel(_))

  // we used to only setIteratorSupplier() if there were no ids OR the first id was instanceof Element,
  // but that allowed the filter in g.V(v).has('k','v') to be ignored.  this created problems for
  // PartitionStrategy which wants to prevent someone from passing "v" from one TraversalSource to
  // another TraversalSource using a different partition
//  ((Graph)this.getTraversal().getGraph().get()).vertices(this.ids):((Graph)this.getTraversal().getGraph().get()).edges(this.ids);
  val s2graph: S2Graph = getTraversal().getGraph().get().asInstanceOf[S2Graph]
  val newIteratorSupplier =
    if (classOf[Vertex].isAssignableFrom(this.returnClass)) vertices()
    else edges()

  setIteratorSupplier(new Supplier[util.Iterator[E]] {
    override def get(): util.Iterator[E] = newIteratorSupplier
  })

  private def vertices(): util.Iterator[E] = {
    val ls = new util.ArrayList[E]()
    ls.iterator()
  }

  private def edges(): util.Iterator[E] = {
    val ls = new util.ArrayList[E]()
    ls.iterator()
  }
  override def getHasContainers: util.List[HasContainer] = ???

  override def addHasContainer(hasContainer: HasContainer): Unit = ???
}
