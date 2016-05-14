package org.apache.s2graph.core.tinkerpop.structure

import org.apache.s2graph.core.TestCommonWithModels
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
import org.scalatest.{Matchers, FunSuite}
import scala.collection.JavaConversions._

class S2GraphTest extends FunSuite with Matchers with TestCommonWithModels {

  initTests()

  val s2 = S2Graph.toS2Graph(graph)

//  test("test S2Graph#vertices.") {
//    s2.vertices().hasNext shouldBe false
//  }
  test("test S2Graph#addVertex.") {
    val srcId = java.lang.Long.valueOf(10L)
    val tgtId = java.lang.Long.valueOf(100L)
    val params = ElementHelper.asMap("id", srcId,
    "serviceName", label.srcService.serviceName, "columnName", label.srcColumnName).toSeq
    val vertex = s2.addVertex(params: _*)

    val tgtParams = ElementHelper.asMap("id", tgtId,
    "serviceName", label.tgtService.serviceName, "columnName", label.tgtColumnName).toSeq

    val tgtVertex = s2.addVertex(tgtParams: _*)
    val edge = vertex.addEdge(label.label, tgtVertex)
    logger.debug(s"[Edge]: ${edge.id}")
  }

}
