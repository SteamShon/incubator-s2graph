package org.apache.s2graph.http

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.apache.s2graph.core.S2Graph
import org.apache.s2graph.core.rest.RequestParser
import org.slf4j.LoggerFactory

object S2GraphMutateRoute {

}

trait S2GraphMutateRoute {

  val s2graph: S2Graph
  val logger = LoggerFactory.getLogger(this.getClass)

//  implicit lazy val ec = s2graph.ec
//  lazy val requestParser = new RequestParser(s2graph)

  lazy val mutateVertex =
    path("vertex") {
      path("insert") {
        complete(HttpResponse(status = StatusCodes.OK,
          entity = HttpEntity(ContentTypes.`application/json`, "insert")
        ))
      }
    }

  // expose routes
  lazy val mutateRoute: Route =
    post {
      concat(
        mutateVertex
      )
    }

}

