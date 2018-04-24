package org.apache.s2graph.core.model

import java.io.File

import com.spotify.annoy.{ANNIndex, IndexType}
import com.typesafe.config.Config
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core._

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.Try

object AnnoyModelFetcher {
  val IndexFileRemotePathKey = "annoy.index.file.remote.path"
  val IndexFilePathKey = "annoy.index.file.path"
  val DictFileRemotePathKey = "annoy.dict.file.remote.path"
  val DictFilePathKey = "annoy.dict.file.path"
  val DimensionKey = "annoy.index.dimension"
  val IndexTypeKey = "annoy.index.type"

  def loadDictFromLocal(file: File): Array[String] = {
    Source.fromFile(file).getLines().map { line =>
      line.stripMargin
    }.toArray
  }

  def buildIndex(config: Config): ANNIndexWithDict = {
    val filePath = config.getString(IndexFilePathKey)
    val dictPath = config.getString(DictFilePathKey)

    val dimension = config.getInt(DimensionKey)
    val indexType = Try { config.getString(IndexTypeKey) }.toOption.map(IndexType.valueOf).getOrElse(IndexType.ANGULAR)

    val dict = loadDictFromLocal(new File(dictPath))
    val index = new ANNIndex(dimension, filePath, indexType)
    ANNIndexWithDict(index, dict)
  }
}

case class ANNIndexWithDict(index: ANNIndex, dict: Array[String]) {
  val dictRev = dict.zipWithIndex.toMap
}

class AnnoyModelFetcher(val graph: S2GraphLike) extends Fetcher {
  import scala.collection.JavaConverters._
  val builder = graph.elementBuilder

  var model: ANNIndexWithDict = _

  override def init(config: Config)(implicit ec: ExecutionContext): Future[Fetcher] = {
    Future {
      model = AnnoyModelFetcher.buildIndex(config)

      this
    }
  }

  /** Fetch **/
  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    val stepResultLs = queryRequests.map { queryRequest =>
      val vertex = queryRequest.vertex
      val queryParam = queryRequest.queryParam

      val srcIndexOpt = model.dictRev.get(vertex.innerId.toIdString())

      srcIndexOpt.map { srcIdx =>
        val srcVector = model.index.getItemVector(srcIdx)
        val nns = model.index.getNearest(srcVector, queryParam.limit).asScala

        val edges = nns.map { tgtIdx =>
          val tgtVertexId = builder.newVertexId(queryParam.label.service,
            queryParam.label.tgtColumnWithDir(queryParam.labelWithDir.dir), model.dict(tgtIdx))
          val tgtVertex = builder.newVertex(tgtVertexId)

          builder.newEdge(vertex, tgtVertex, queryParam.label, queryParam.dir.toInt, propsWithTs = S2Edge.EmptyState)
        }
        val edgeWithScores = edges.map(e => EdgeWithScore(e, 1.0, queryParam.label))
        StepResult(edgeWithScores, Nil, Nil)
      }.getOrElse(StepResult.Empty)
    }

    Future.successful(stepResultLs)
  }

  override def close(): Unit = {
    // do clean up
  }
}
