package org.apache.s2graph.core.tinkerpop.structure

import org.apache.tinkerpop.gremlin.structure.T
import org.scalatest._

class S2GraphTest extends FunSuite with Matchers with IntegrateTinkerpopCommon {



  test("addVertex") {
    g.addVertex("serviceName", TestUtil.testServiceName,
    "columnName", TestUtil.testColumnName,
    T.id, Int.box(10))

  }
}
