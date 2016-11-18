package org.apache.s2graph.core.tinkerpop.structure

import org.apache.s2graph.core.mysqls.{ServiceColumn, Service}
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.structure.Direction
import org.scalatest._

class S2GraphTest extends FunSuite with Matchers with IntegrateTinkerpopCommon {
  import TestUtil._

  var testService: Service = _
  var testColumn: ServiceColumn = _

  override def beforeAll = {
    super.beforeAll()
    testService = Service.findByName(testServiceName, useCache = false).getOrElse(throw new RuntimeException("Service is not found."))
    testColumn = ServiceColumn.find(testService.id.get, testColumnName, useCache = false).getOrElse(throw new RuntimeException("column is not found."))
  }

  test("addVertex") {

    val srcV = g.addVertex("service", testService, "column", testColumn, "id", Int.box(10))
    val iter = g.vertices("service", testService, "column", testColumn, "id", Int.box(10))

    iter.hasNext should be(true)
    val actualSrcV = iter.next()
    actualSrcV should be(srcV)
  }

  test("addEdge") {
    val srcV = g.addVertex("service", testService, "column", testColumn, "id", Int.box(10))
    val tgtV = g.addVertex("service", testService, "column", testColumn, "id", Int.box(20))
    val edge = srcV.addEdge(testLabelName, tgtV)
    logger.error(s"[ServiceColumn.id]: ${testColumn.id.get}")
    val t = g.traversal().V("service", testService, "column", testColumn, "id", Int.box(10)).outE(testLabelName)
    val edges = t.toList
    edges.size() should be(1)
    edges.get(0) should be(edge)
  }
}
