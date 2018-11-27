package org.apache.s2graph.http

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import com.fasterxml.jackson.core.JsonParseException
import org.apache.s2graph.core.S2Graph
import org.apache.s2graph.core.rest.RequestParser
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.ExecutionContext

object S2GraphMutateRoute {

}

trait S2GraphMutateRoute {

  val s2graph: S2Graph
  val logger = LoggerFactory.getLogger(this.getClass)
  lazy val parser = new RequestParser(s2graph)

//  lazy val requestParser = new RequestParser(s2graph)
  lazy val exceptionHandler = ExceptionHandler {
    case ex: JsonParseException =>
      complete(StatusCodes.BadRequest -> ex.getMessage)
    case ex: java.lang.IllegalArgumentException =>
      complete(StatusCodes.BadRequest -> ex.getMessage)
  }

  lazy val mutateVertex = path("vertex" / Segments) { params =>
    val (operation, serviceNameOpt, columnNameOpt) = params match {
      case operation :: serviceName :: columnName :: Nil =>
        (operation, Option(serviceName), Option(columnName))
      case operation :: Nil =>
        (operation, None, None)
    }

    entity(as[String]) { body =>
      val payload = Json.parse(body)

      implicit val ec = s2graph.ec

      val future = tryMutates(payload, operation, serviceNameOpt, columnNameOpt).map { mutateResponses =>
        HttpResponse(
          status = StatusCodes.OK,
          entity = HttpEntity(ContentTypes.`application/json`, Json.toJson(mutateResponses).toString)
        )
      }

      complete(future)
    }
  }

  def tryMutates(jsValue: JsValue,
                 operation: String,
                 serviceNameOpt: Option[String] = None,
                 columnNameOpt: Option[String] = None,
                 withWait: Boolean = true)(implicit ec: ExecutionContext) = {
    val vertices = parser.toVertices(jsValue, operation, serviceNameOpt, columnNameOpt)

    val verticesToStore = vertices.filterNot(_.isAsync)

    s2graph.mutateVertices(verticesToStore, withWait).map(_.map(_.isSuccess))
  }

  // expose routes
  lazy val mutateRoute: Route =
    post {
      concat(
        handleExceptions(exceptionHandler) {
          mutateVertex
        }
      )
    }

}

