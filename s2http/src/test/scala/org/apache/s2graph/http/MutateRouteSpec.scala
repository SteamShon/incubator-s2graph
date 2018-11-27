package org.apache.s2graph.http

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core.S2Graph
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsString, Json}

class MutateRouteSpec extends WordSpec with Matchers with ScalaFutures with ScalatestRouteTest with S2GraphMutateRoute with BeforeAndAfterAll {
  val config = ConfigFactory.load()
  val s2graph = new S2Graph(config)
  override val logger = LoggerFactory.getLogger(this.getClass)

  override def afterAll(): Unit = {
    s2graph.shutdown()
  }

  import akka.http.scaladsl.server.Directives._

  lazy val routes = mutateRoute

  val serviceName = "kakaoFavorites"
  val columnName = "userName"

  "MutateRoute" should {
    "be able to insert vertex (POST /mutate/vertex/insert)" in {
//      {"timestamp": 10, "serviceName": "s2graph", "columnName": "user", "id": 1, "props": {}}
      val param = Json.obj(
        "timestamp" -> 10,
        "serviceName" -> serviceName,
        "columnName" -> columnName,
        "id" -> "user_a",
        "props" -> Json.obj(
          "age" -> 20
        )
      )
      val entity = Marshal(param.toString).to[MessageEntity].futureValue
      val request = Post("/vertex/insert").withEntity(entity)

      request ~> routes ~> check {
        status should ===(StatusCodes.OK)
        contentType should ===(ContentTypes.`application/json`)

        val response = Json.parse(entityAs[String])
        response should ===(Json.toJson(Seq(true)))
      }
    }
  }
}

