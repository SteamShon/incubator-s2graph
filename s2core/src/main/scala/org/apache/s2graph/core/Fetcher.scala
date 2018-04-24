package org.apache.s2graph.core

import org.apache.s2graph.core.types.VertexId

import scala.concurrent.{ExecutionContext, Future}

trait Fetcher {
  def fetches(queryRequests: Seq[QueryRequest],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]]
}
