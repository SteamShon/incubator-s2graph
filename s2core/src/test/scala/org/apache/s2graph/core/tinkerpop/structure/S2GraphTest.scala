package org.apache.s2graph.core.tinkerpop.structure

import java.util.concurrent.TimeUnit

import org.apache.s2graph.core.{Vertex, TestCommonWithModels}
import org.apache.s2graph.core.tinkerpop.process.S2GraphStepStrategy
import org.apache.s2graph.core.tinkerpop.process.traversal.step.sideEffect.S2GraphStep
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
import org.scalatest.{Matchers, FunSuite}
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

class S2GraphTest extends FunSuite with Matchers with TestCommonWithModels {

  initTests()

  val timeout = Duration(1, TimeUnit.SECONDS)
  val s2 = S2Graph.toS2Graph(graph)
  val srcId = java.lang.Long.valueOf(10L)
  val tgtId = java.lang.Long.valueOf(100L)

  val params = ElementHelper.asMap("id", srcId,
    "serviceName", label.srcServiceName, "columnName", label.srcColumnName).toSeq
//  val vertex = s2.addVertex(params: _*)

  val tgtParams = ElementHelper.asMap("id", tgtId,
    "serviceName", label.tgtServiceName, "columnName", label.tgtColumnName).toSeq

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
  test("test S2Graph#addVertexStep explain.") {
    val v = s2.traversal().clone().addV().property("serviceName", label.srcServiceName)
      .property("columnName", label.srcColumnName).property("id", srcId).explain()

    logger.debug(s"v: ${v.toString}")
    // note that explain seems last step and not actually run traversal.
  }
  test("test S2Graph#addVertexStep.") {
    val v = s2.traversal().clone().addV().property("serviceName", label.srcServiceName)
      .property("columnName", label.srcColumnName).property("id", srcId).next()

    logger.debug(s"v: ${v.toString}")

    /** since traversal for query is not yet implemented, we are using s2graph native
      * graph methods, not TinkerPop Traversal yet. this should be changed.
      */
    val checkFuture = s2.client.getVertices(Seq(v.asInstanceOf[S2Vertex].vertex)).map { fetchedVertices =>
      fetchedVertices.nonEmpty should be(true)
      fetchedVertices.head.id should be(v.id())
    }
    Await.result(checkFuture, timeout)
  }

}
