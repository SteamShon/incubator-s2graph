package org.apache.s2graph.core.storage.dynamo

import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core._

import scala.concurrent.{ExecutionContext, Future}

class DynamoDBEdgeFetcher extends EdgeFetcher {
  override def fetches(queryRequests: Seq[QueryRequest], prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = ???

  override def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2EdgeLike]] = ???
}
