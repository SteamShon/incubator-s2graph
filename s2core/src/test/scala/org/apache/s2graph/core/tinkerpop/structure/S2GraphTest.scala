package org.apache.s2graph.core.tinkerpop.structure

import java.util.concurrent.TimeUnit

import org.apache.s2graph.core.TestCommonWithModels
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class S2GraphTest extends FunSuite with Matchers with TestCommonWithModels {

  initTests()

  val timeout = Duration(1, TimeUnit.SECONDS)
  val s2 = S2Graph.toS2Graph(graph)
  val srcId = java.lang.Long.valueOf(10L)
  val tgtId = java.lang.Long.valueOf(100L)

  val params = ElementHelper.asMap("id", srcId,
    "serviceName", label.srcServiceName, "columnName", label.srcColumnName).toSeq

  val tgtParams = ElementHelper.asMap("id", tgtId,
    "serviceName", label.tgtServiceName, "columnName", label.tgtColumnName).toSeq



  test("test S2Graph#addVertexStep explain.") {
    val v = s2.traversal().clone().addV("id", srcId, "serviceName", label.srcServiceName, "columnName", label.srcColumnName).explain()

    logger.debug(s"v: ${v.toString}")
  }
  test("test S2Graph#addVertexStep.") {
    val v = s2.traversal().clone().addV("id", srcId, "serviceName", label.srcServiceName, "columnName", label.srcColumnName).next()

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

  test("test S2Graph#addEdgeStep explain.") {
    val e = s2.traversal().clone().
      addV("id", srcId, "serviceName", label.srcServiceName, "columnName", label.srcColumnName).as("from").
      addV("id", tgtId, "serviceName", label.tgtServiceName, "columnName", label.tgtColumnName).as("to").addE(label.label)
      .from("from").to("to").explain()

    logger.debug(s"v: ${e.toString}")
  }

  test("test S2Graph#addEdgeStep.") {
    val e = s2.traversal().clone().
      addV("id", srcId, "serviceName", label.srcServiceName, "columnName", label.srcColumnName).as("from").
      addV("id", tgtId, "serviceName", label.tgtServiceName, "columnName", label.tgtColumnName).as("to").addE(label.label)
      .from("from").to("to").next()

    logger.debug(s"v: ${e.toString}")

    /** since traversal for query is not yet implemented, we are using s2graph native
      * graph methods, not TinkerPop Traversal yet. this should be changed.
      */
    val queryJson = Json.parse(
      s"""
         |{
         |	"srcVertices": [{
         |		"serviceName": "${label.serviceName}",
         |		"columnName": "${label.srcColumnName}",
         |		"id": ${srcId}
         |	}],
         |	"steps": [{
         |		"step": [{
         |			"label": "${label.label}",
         |      "direction": "out"
         |		}]
         |	}]
         |}
       """.stripMargin)

    val query = parser.toQuery(queryJson)
    val checkFuture = s2.client.getEdges(query).map { fetchedResult =>
      fetchedResult.nonEmpty should be(true)
      val result = fetchedResult.head
      result.queryResult.edgeWithScoreLs.find(edgeWithScore => edgeWithScore.edge.id == e.id())
    }
    Await.result(checkFuture, timeout)

  }


  test("test S2Graph#getEdges.") {
    val edge = s2.traversal().clone().
      addV("id", srcId, "serviceName", label.srcServiceName, "columnName", label.srcColumnName).as("from").
      addV("id", tgtId, "serviceName", label.tgtServiceName, "columnName", label.tgtColumnName).as("to").addE(label.label)
      .from("from").to("to").next()

    logger.debug(s"$edge")
    val fetchedEdges = s2.traversal().clone().V(label.srcServiceName, label.srcColumnName, srcId).outE(label.label).next(10)

    fetchedEdges.size should be(2)
    val degreeEdge = fetchedEdges(0).asInstanceOf[S2Edge]
    val fetchedEdge = fetchedEdges(1).asInstanceOf[S2Edge]
    logger.debug(s"[DegreeEdge]: ${degreeEdge.coreEdge.toLogString}")
    logger.debug(s"[FetchedEdge]: ${fetchedEdge.coreEdge.toLogString}")
    fetchedEdge.id == edge.id should be(true)
  }
}
