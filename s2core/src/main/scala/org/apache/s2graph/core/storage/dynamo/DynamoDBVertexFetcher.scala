package org.apache.s2graph.core.storage.dynamo

import org.apache.s2graph.core.{S2VertexLike, VertexFetcher, VertexQueryParam}

import scala.concurrent.{ExecutionContext, Future}

class DynamoDBVertexFetcher extends VertexFetcher {
  override def fetchVertices(vertexQueryParam: VertexQueryParam)(implicit ec: ExecutionContext): Future[Seq[S2VertexLike]] = ???

  override def fetchVerticesAll()(implicit ec: ExecutionContext): Future[Seq[S2VertexLike]] = ???
}
