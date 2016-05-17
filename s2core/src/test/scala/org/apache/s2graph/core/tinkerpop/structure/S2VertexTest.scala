package org.apache.s2graph.core.tinkerpop.structure


import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.mysqls.{Service, Label, ServiceColumn}
import org.apache.s2graph.core.{Graph, Management, TestCommonWithModels}
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
import org.scalatest.{Matchers, FunSuite}
import scala.concurrent.ExecutionContext
import scala.collection.JavaConversions._

class S2VertexTest extends FunSuite with Matchers with TestCommonWithModels {


  initTests()

  override val serviceName = "s2graph"
  override val columnName = "user_id"

  override val service = Service(id = Option(1), serviceName = serviceName,
    accessToken = "", cluster = "", hTableName = "",
    preSplitSize = 0, hTableTTL = None)

  val serviceColumn = ServiceColumn(id = Option(1),
    serviceId = service.id.get, columnName = "user_id", columnType = "string",
    schemaVersion = "v4")

  val _label = Label(id = Option(1), label = "test_label",
    srcServiceId = serviceColumn.id.get, srcColumnName = serviceColumn.columnName, srcColumnType = serviceColumn.columnType,
    tgtServiceId = serviceColumn.id.get, tgtColumnName = serviceColumn.columnName, tgtColumnType = serviceColumn.columnType,
    isDirected = true, serviceName = service.serviceName, serviceId = service.id.get,
    consistencyLevel = "strong", hTableName = "", hTableTTL = None, schemaVersion = "v4", isAsync = false, compressionAlgorithm = "")



  override def initTests() = {
    config = ConfigFactory.load()
    graph = new Graph(config)(ExecutionContext.Implicits.global)
    management = new Management(graph)
  }

  test("test id") {
    val s2graph = S2Graph.toS2Graph(graph)
    // note that column has long type for user_id
    val params = ElementHelper.asMap("id", java.lang.Long.valueOf(101L)).toSeq
    val coreVertex =
      Management.toVertexWithServiceColumn(serviceColumn)("101")(params: _*)

//    val coreVertex =
      Management.toVertexWithServiceColumn(serviceColumn)("102")() // WrappedArray

    val s2Vertex = s2graph.toS2Vertex(coreVertex)

    s2Vertex.id() should be(coreVertex.id)
    s2Vertex.vertex should be(coreVertex)
  }
}
