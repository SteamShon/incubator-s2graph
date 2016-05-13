package org.apache.s2graph.core.tinkerpop.structure

import org.apache.s2graph.core.TestCommonWithModels
import org.scalatest.{Matchers, FunSuite}

class S2GraphTest extends FunSuite with Matchers with TestCommonWithModels {

  initTests()

  test("create S2Graph class from core.Graph") {
    val s2graph = S2Graph.toS2Graph(graph)
    s2graph.vertices().hasNext shouldBe false
  }
}
