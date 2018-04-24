package org.apache.s2graph.core.model

import com.typesafe.config.Config
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Reference implementation for Fetcher interface.
  * it only produce constant edges.
  */
class MemoryModelFetcher(val graph: S2GraphLike) extends Fetcher {
  val builder = graph.elementBuilder
  val ranges = (0 to 10)

  override def init(config: Config): Future[Fetcher] = {
    Future.successful(this)
  }

  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    val stepResultLs = queryRequests.map { queryRequest =>
      val queryParam = queryRequest.queryParam
      val edges = ranges.map { ith =>
        val tgtVertexId = builder.newVertexId(queryParam.label.service, queryParam.label.tgtColumnWithDir(queryParam.labelWithDir.dir), ith.toString)
        val tgtVertex = builder.newVertex(tgtVertexId)

        builder.newEdge(queryRequest.vertex, tgtVertex, queryParam.label, queryParam.dir.toInt, propsWithTs = S2Edge.EmptyState)
      }

      val edgeWithScores = edges.map(e => EdgeWithScore(e, 1.0, queryParam.label))
      StepResult(edgeWithScores, Nil, Nil)
    }

    Future.successful(stepResultLs)
  }

  override def close(): Unit = {}
}
