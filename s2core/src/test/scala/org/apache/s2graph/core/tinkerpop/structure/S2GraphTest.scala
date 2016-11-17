package org.apache.s2graph.core.tinkerpop.structure

import org.apache.s2graph.core.mysqls.{ServiceColumn, Service}
import org.apache.s2graph.core.utils.logger
import org.scalatest._

class S2GraphTest extends FunSuite with Matchers with IntegrateTinkerpopCommon {
  import TestUtil._



  test("addVertex") {
    initTestData()

    val testService = Service.findByName(testServiceName).getOrElse(throw new RuntimeException("Service is not found."))
    val testColumn = ServiceColumn.find(testService.id.get, testColumnName).getOrElse(throw new RuntimeException("column is not found."))


    val srcV = g.addVertex("service", testService, "column", testColumn, "id", Int.box(10))
    val tgtV = g.addVertex("service", testService, "column", testColumn, "id", Int.box(20))
    val edge = srcV.addEdge(testLabelName, tgtV)
    logger.error(s"$edge")
  }
}
