package org.apache.s2graph.core.tinkerpop.structure

import org.apache.s2graph.core.{Vertex, TestCommonWithModels}
import org.apache.s2graph.core.tinkerpop.process.S2GraphStepStrategy
import org.apache.s2graph.core.tinkerpop.process.traversal.step.sideEffect.S2GraphStep
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
import org.scalatest.{Matchers, FunSuite}
import scala.collection.JavaConversions._

class S2GraphTest extends FunSuite with Matchers with TestCommonWithModels {

  initTests()

  val s2 = S2Graph.toS2Graph(graph)
  val srcId = java.lang.Long.valueOf(10L)
  val tgtId = java.lang.Long.valueOf(100L)

  val params = ElementHelper.asMap("id", srcId,
    "serviceName", label.srcService.serviceName, "columnName", label.srcColumnName).toSeq
//  val vertex = s2.addVertex(params: _*)

  val tgtParams = ElementHelper.asMap("id", tgtId,
    "serviceName", label.tgtService.serviceName, "columnName", label.tgtColumnName).toSeq

//  val tgtVertex = s2.addVertex(tgtParams: _*)

//  val edge = vertex.addEdge(label.label, tgtVertex)

//  logger.debug(s"[Edge]: ${edge.id}")
//  test("test S2Graph#vertices.") {
//    s2.vertices().hasNext shouldBe false
//  }
  test("test S2Graph#hasStep.") {
//    val traversal = s2.traversal().V().has("name", srcId).asAdmin()
//    traversal.applyStrategies()
//    val size = traversal.getSteps().size()
//    logger.debug(s"[Size]: $size")
//    traversal.getEndStep.getLabels().isEmpty should be(true)
//    traversal.getEndStep.getClass == classOf[HasStep[_]] should be(true)
//    logger.debug(s"${s2.traversal().getStrategies}")
  }
  test("test S2Graph#addVertexStep.") {
    var newV: org.apache.tinkerpop.gremlin.structure.Vertex = null
    val traversal = s2.traversal().addV().property("serviceName", label.srcService.serviceName)
      .property("columnName", label.srcColumnName).property("id", srcId)
      .asAdmin()
    traversal.applyStrategies()
    for {
      step <- traversal.getEndStep
    } {
      newV = step.get()
      logger.debug(s"[ResultVertex]: ${step.get().id()}")
    }
    val t = s2.traversal().V(newV).asAdmin()
    t.applyStrategies()
    for {
      step <- traversal.getEndStep
    } {
      step.get() == newV should be(true)
      logger.debug(s"[NewV]: ${newV.id()}")
      logger.debug(s"[FetchedV]: ${step.get().id()}")
    }
  }

}
