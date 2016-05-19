/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.s2graph.core.mysqls.{Label, Model}
import org.apache.s2graph.core.parsers.WhereParser
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorage
import org.apache.s2graph.core.types.{InnerVal, LabelWithDirection}
import org.apache.s2graph.core.utils.logger
import JSONParser._
import play.api.libs.json.{JsObject, Json}
import scala.collection.JavaConversions._
import scala.collection._
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.util.Try
import java.util
import java.util.concurrent.ConcurrentHashMap


object Graph {
  val DefaultScore = 1.0

  private val DefaultConfigs: Map[String, AnyRef] = Map(
    "hbase.zookeeper.quorum" -> "localhost",
    "hbase.table.name" -> "s2graph",
    "hbase.table.compression.algorithm" -> "gz",
    "phase" -> "dev",
    "db.default.driver" -> "com.mysql.jdbc.Driver",
    "db.default.url" -> "jdbc:mysql://localhost:3306/graph_dev",
    "db.default.password" -> "graph",
    "db.default.user" -> "graph",
    "cache.max.size" -> java.lang.Integer.valueOf(10000),
    "cache.ttl.seconds" -> java.lang.Integer.valueOf(60),
    "hbase.client.retries.number" -> java.lang.Integer.valueOf(20),
    "hbase.rpcs.buffered_flush_interval" -> java.lang.Short.valueOf(100.toShort),
    "hbase.rpc.timeout" -> java.lang.Integer.valueOf(1000),
    "max.retry.number" -> java.lang.Integer.valueOf(100),
    "lock.expire.time" -> java.lang.Integer.valueOf(1000 * 60 * 10),
    "max.back.off" -> java.lang.Integer.valueOf(100),
    "hbase.fail.prob" -> java.lang.Double.valueOf(-0.1),
    "delete.all.fetch.size" -> java.lang.Integer.valueOf(1000),
    "future.cache.max.size" -> java.lang.Integer.valueOf(100000),
    "future.cache.expire.after.write" -> java.lang.Integer.valueOf(10000),
    "future.cache.expire.after.access" -> java.lang.Integer.valueOf(5000),
    "s2graph.storage.backend" -> "hbase"
  )

  var DefaultConfig: Config = ConfigFactory.parseMap(DefaultConfigs)

  /** helpers for filterEdges */
  type HashKey = (Int, Int, Int, Int, Boolean)
  type FilterHashKey = (Int, Int)
  type Result = (ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]],
    ConcurrentHashMap[HashKey, (FilterHashKey, Edge, Double)],
    ListBuffer[(HashKey, FilterHashKey, Edge, Double)])

  def toHashKey(queryParam: QueryParam, edge: Edge, isDegree: Boolean): (HashKey, FilterHashKey) = {
    val src = edge.srcVertex.innerId.hashCode()
    val tgt = edge.tgtVertex.innerId.hashCode()
    val hashKey = (src, edge.labelWithDir.labelId, edge.labelWithDir.dir, tgt, isDegree)
    val filterHashKey = (src, tgt)

    (hashKey, filterHashKey)
  }

  def alreadyVisitedVertices(queryResultLs: Seq[QueryResult]): Map[(LabelWithDirection, Vertex), Boolean] = {
    val vertices = for {
      queryResult <- queryResultLs
      edgeWithScore <- queryResult.edgeWithScoreLs
      edge = edgeWithScore.edge
      vertex = if (edge.labelWithDir.dir == GraphUtil.directions("out")) edge.tgtVertex else edge.srcVertex
    } yield (edge.labelWithDir, vertex) -> true

    vertices.toMap
  }

  /** common methods for filter out, transform, aggregate queryResult */
  def convertEdges(queryParam: QueryParam, edge: Edge, nextStepOpt: Option[Step]): Seq[Edge] = {
    for {
      convertedEdge <- queryParam.transformer.transform(edge, nextStepOpt) if !edge.isDegree
    } yield convertedEdge
  }

  def processTimeDecay(queryParam: QueryParam, edge: Edge) = {
    /** process time decay */
    val tsVal = queryParam.timeDecay match {
      case None => 1.0
      case Some(timeDecay) =>
        val tsVal = try {
          val labelMeta = edge.label.metaPropsMap(timeDecay.labelMetaSeq)
          val innerValWithTsOpt = edge.propsWithTs.get(timeDecay.labelMetaSeq)
          innerValWithTsOpt.map { innerValWithTs =>
            val innerVal = innerValWithTs.innerVal
            labelMeta.dataType match {
              case InnerVal.LONG => innerVal.value match {
                case n: BigDecimal => n.bigDecimal.longValue()
                case _ => innerVal.toString().toLong
              }
              case _ => innerVal.toString().toLong
            }
          } getOrElse(edge.ts)
        } catch {
          case e: Exception =>
            logger.error(s"processTimeDecay error. ${edge.toLogString}", e)
            edge.ts
        }
        val timeDiff = queryParam.timestamp - tsVal
        timeDecay.decay(timeDiff)
    }

    tsVal
  }

  def aggregateScore(newScore: Double,
                     resultEdges: ConcurrentHashMap[HashKey, (FilterHashKey, Edge, Double)],
                     duplicateEdges: ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]],
                     edgeWithScoreSorted: ListBuffer[(HashKey, FilterHashKey, Edge, Double)],
                     hashKey: HashKey,
                     filterHashKey: FilterHashKey,
                     queryParam: QueryParam,
                     convertedEdge: Edge) = {

    /** skip duplicate policy check if consistencyLevel is strong */
    if (queryParam.label.consistencyLevel != "strong" && resultEdges.containsKey(hashKey)) {
      val (oldFilterHashKey, oldEdge, oldScore) = resultEdges.get(hashKey)
      //TODO:
      queryParam.duplicatePolicy match {
        case Query.DuplicatePolicy.First => // do nothing
        case Query.DuplicatePolicy.Raw =>
          if (duplicateEdges.containsKey(hashKey)) {
            duplicateEdges.get(hashKey).append(convertedEdge -> newScore)
          } else {
            val newBuffer = new ListBuffer[(Edge, Double)]
            newBuffer.append(convertedEdge -> newScore)
            duplicateEdges.put(hashKey, newBuffer)
          }
        case Query.DuplicatePolicy.CountSum =>
          resultEdges.put(hashKey, (filterHashKey, oldEdge, oldScore + 1))
        case _ =>
          resultEdges.put(hashKey, (filterHashKey, oldEdge, oldScore + newScore))
      }
    } else {
      resultEdges.put(hashKey, (filterHashKey, convertedEdge, newScore))
      edgeWithScoreSorted.append((hashKey, filterHashKey, convertedEdge, newScore))
    }
  }

  def aggregateResults(queryRequestWithResult: QueryRequestWithResult,
                       queryParamResult: Result,
                       edgesToInclude: util.HashSet[FilterHashKey],
                       edgesToExclude: util.HashSet[FilterHashKey]): QueryRequestWithResult = {
    val (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
    val (query, stepIdx, _, queryParam) = QueryRequest.unapply(queryRequest).get

    val (duplicateEdges, resultEdges, edgeWithScoreSorted) = queryParamResult
    val edgesWithScores = for {
      (hashKey, filterHashKey, edge, _) <- edgeWithScoreSorted if !edgesToExclude.contains(filterHashKey) || edgesToInclude.contains(filterHashKey)
      score = resultEdges.get(hashKey)._3
      (duplicateEdge, aggregatedScore) <- fetchDuplicatedEdges(edge, score, hashKey, duplicateEdges) if aggregatedScore >= queryParam.threshold
    } yield EdgeWithScore(duplicateEdge, aggregatedScore)

    QueryRequestWithResult(queryRequest, QueryResult(edgesWithScores))
  }

  def fetchDuplicatedEdges(edge: Edge,
                           score: Double,
                           hashKey: HashKey,
                           duplicateEdges: ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]]) = {
    (edge -> score) +: (if (duplicateEdges.containsKey(hashKey)) duplicateEdges.get(hashKey) else Seq.empty)
  }

  def queryResultWithFilter(queryRequestWithResult: QueryRequestWithResult) = {
    val (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
    val (_, _, _, queryParam) = QueryRequest.unapply(queryRequest).get
    val whereFilter = queryParam.where.get
    if (whereFilter == WhereParser.success) queryResult.edgeWithScoreLs
    else queryResult.edgeWithScoreLs.withFilter(edgeWithScore => whereFilter.filter(edgeWithScore.edge))
  }

  def filterEdges(queryResultLsFuture: Future[Seq[QueryRequestWithResult]],
                  alreadyVisited: Map[(LabelWithDirection, Vertex), Boolean] = Map.empty[(LabelWithDirection, Vertex), Boolean])
                 (implicit ec: scala.concurrent.ExecutionContext): Future[Seq[QueryRequestWithResult]] = {

    queryResultLsFuture.map { queryRequestWithResultLs =>
      if (queryRequestWithResultLs.isEmpty) Nil
      else {
        val (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResultLs.head).get
        val (q, stepIdx, srcVertex, queryParam) = QueryRequest.unapply(queryRequest).get
        val step = q.steps(stepIdx)

        val nextStepOpt = if (stepIdx < q.steps.size - 1) Option(q.steps(stepIdx + 1)) else None

        val excludeLabelWithDirSet = new util.HashSet[(Int, Int)]
        val includeLabelWithDirSet = new util.HashSet[(Int, Int)]
        step.queryParams.filter(_.exclude).foreach(l => excludeLabelWithDirSet.add(l.labelWithDir.labelId -> l.labelWithDir.dir))
        step.queryParams.filter(_.include).foreach(l => includeLabelWithDirSet.add(l.labelWithDir.labelId -> l.labelWithDir.dir))

        val edgesToExclude = new util.HashSet[FilterHashKey]()
        val edgesToInclude = new util.HashSet[FilterHashKey]()

        val queryParamResultLs = new ListBuffer[Result]
        queryRequestWithResultLs.foreach { queryRequestWithResult =>
          val (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
          val queryParam = queryRequest.queryParam
          val duplicateEdges = new util.concurrent.ConcurrentHashMap[HashKey, ListBuffer[(Edge, Double)]]()
          val resultEdges = new util.concurrent.ConcurrentHashMap[HashKey, (FilterHashKey, Edge, Double)]()
          val edgeWithScoreSorted = new ListBuffer[(HashKey, FilterHashKey, Edge, Double)]
          val labelWeight = step.labelWeights.getOrElse(queryParam.labelWithDir.labelId, 1.0)

          // store degree value with Array.empty so if degree edge exist, it comes at very first.
          def checkDegree() = queryResult.edgeWithScoreLs.headOption.exists { edgeWithScore =>
            edgeWithScore.edge.isDegree
          }
          var isDegree = checkDegree()

          val includeExcludeKey = queryParam.labelWithDir.labelId -> queryParam.labelWithDir.dir
          val shouldBeExcluded = excludeLabelWithDirSet.contains(includeExcludeKey)
          val shouldBeIncluded = includeLabelWithDirSet.contains(includeExcludeKey)

          queryResultWithFilter(queryRequestWithResult).foreach { edgeWithScore =>
            val (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
            if (queryParam.transformer.isDefault) {
              val convertedEdge = edge

              val (hashKey, filterHashKey) = toHashKey(queryParam, convertedEdge, isDegree)

              /** check if this edge should be exlcuded. */
              if (shouldBeExcluded && !isDegree) {
                edgesToExclude.add(filterHashKey)
              } else {
                if (shouldBeIncluded && !isDegree) {
                  edgesToInclude.add(filterHashKey)
                }
                val tsVal = processTimeDecay(queryParam, convertedEdge)
                val newScore = labelWeight * score * tsVal
                aggregateScore(newScore, resultEdges, duplicateEdges, edgeWithScoreSorted, hashKey, filterHashKey, queryParam, convertedEdge)
              }
            } else {
              convertEdges(queryParam, edge, nextStepOpt).foreach { convertedEdge =>
                val (hashKey, filterHashKey) = toHashKey(queryParam, convertedEdge, isDegree)

                /** check if this edge should be exlcuded. */
                if (shouldBeExcluded && !isDegree) {
                  edgesToExclude.add(filterHashKey)
                } else {
                  if (shouldBeIncluded && !isDegree) {
                    edgesToInclude.add(filterHashKey)
                  }
                  val tsVal = processTimeDecay(queryParam, convertedEdge)
                  val newScore = labelWeight * score * tsVal
                  aggregateScore(newScore, resultEdges, duplicateEdges, edgeWithScoreSorted, hashKey, filterHashKey, queryParam, convertedEdge)
                }
              }
            }
            isDegree = false
          }
          val ret = (duplicateEdges, resultEdges, edgeWithScoreSorted)
          queryParamResultLs.append(ret)
        }

        val aggregatedResults = for {
          (queryRequestWithResult, queryParamResult) <- queryRequestWithResultLs.zip(queryParamResultLs)
        } yield {
            aggregateResults(queryRequestWithResult, queryParamResult, edgesToInclude, edgesToExclude)
          }

        aggregatedResults
      }
    }
  }

  def toGraphElement(graph: Graph, s: String, labelMapping: Map[String, String] = Map.empty): Option[GraphElement] = Try {
    val parts = GraphUtil.split(s)
    val logType = parts(2)
    val element = if (logType == "edge" | logType == "e") {
      /** current only edge is considered to be bulk loaded */
      labelMapping.get(parts(5)) match {
        case None =>
        case Some(toReplace) =>
          parts(5) = toReplace
      }
      toEdge(graph, parts)
    } else if (logType == "vertex" | logType == "v") {
      toVertex(graph, parts)
    } else {
      throw new GraphExceptions.JsonParseException("log type is not exist in log.")
    }

    element
  } recover {
    case e: Exception =>
      logger.error(s"$e", e)
      None
  } get


  def toVertex(graph: Graph, s: String): Option[Vertex] = {
    toVertex(graph, GraphUtil.split(s))
  }

  def toEdge(graph: Graph, s: String): Option[Edge] = {
    toEdge(graph, GraphUtil.split(s))
  }

  def toEdge(graph: Graph, parts: Array[String]): Option[Edge] = Try {
    val (ts, operation, logType, srcId, tgtId, label) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
    val props = if (parts.length >= 7) fromJsonToProperties(Json.parse(parts(6)).asOpt[JsObject].getOrElse(Json.obj())) else Map.empty[String, Any]
    val tempDirection = if (parts.length >= 8) parts(7) else "out"
    val direction = if (tempDirection != "out" && tempDirection != "in") "out" else tempDirection

    val edge = Management.toEdge(graph, ts.toLong, operation, srcId, tgtId, label, direction, props)
    //            logger.debug(s"toEdge: $edge")
    Some(edge)
  } recover {
    case e: Exception =>
      logger.error(s"toEdge: $e", e)
      throw e
  } get

  def toVertex(graph: Graph, parts: Array[String]): Option[Vertex] = Try {
    val (ts, operation, logType, srcId, serviceName, colName) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
    val props = if (parts.length >= 7) fromJsonToProperties(Json.parse(parts(6)).asOpt[JsObject].getOrElse(Json.obj())) else Map.empty[String, Any]
    Some(Management.toVertex(graph, ts.toLong, operation, srcId, serviceName, colName, props))
  } recover {
    case e: Throwable =>
      logger.error(s"toVertex: $e", e)
      throw e
  } get

  def initStorage(graph: Graph, config: Config)(ec: ExecutionContext) = {
    config.getString("s2graph.storage.backend") match {
      case "hbase" => new AsynchbaseStorage(graph, config)(ec)
      case _ => throw new RuntimeException("not supported storage.")
    }
  }
}

class Graph(_config: Config)(implicit val ec: ExecutionContext) {
  val config = _config.withFallback(Graph.DefaultConfig)

  Model.apply(config)
  Model.loadCache()

  // TODO: Make storage client by config param
  val storage = Graph.initStorage(this, config)(ec)


  for {
    entry <- config.entrySet() if Graph.DefaultConfigs.contains(entry.getKey)
    (k, v) = (entry.getKey, entry.getValue)
  } logger.info(s"[Initialized]: $k, ${this.config.getAnyRef(k)}")

  /** select */
  def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryRequestWithResult]] = storage.checkEdges(params)

  def getEdges(q: Query): Future[Seq[QueryRequestWithResult]] = storage.getEdges(q)

  def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = storage.getVertices(vertices)

  /** write */
  def deleteAllAdjacentEdges(srcVertices: List[Vertex], labels: Seq[Label], dir: Int, ts: Long): Future[Boolean] =
    storage.deleteAllAdjacentEdges(srcVertices, labels, dir, ts)

  def mutateElements(elements: Seq[GraphElement], withWait: Boolean = false): Future[Seq[Boolean]] =
    storage.mutateElements(elements, withWait)

  def mutateEdges(edges: Seq[Edge], withWait: Boolean = false): Future[Seq[Boolean]] = storage.mutateEdges(edges, withWait)

  def mutateVertices(vertices: Seq[Vertex], withWait: Boolean = false): Future[Seq[Boolean]] = storage.mutateVertices(vertices, withWait)

  def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]] = storage.incrementCounts(edges, withWait)


  def addEdge(srcId: Any,
              tgtId: Any,
              labelName: String,
              direction: String = "out",
              props: Map[String, Any] = Map.empty,
              ts: Long = System.currentTimeMillis(),
              operation: String = "insert",
              withWait: Boolean = true): Future[Boolean] = {

    val innerEdges = Seq(S2Edge(this, srcId, tgtId, labelName, direction, props.toMap, ts, operation).edge)
    storage.mutateEdges(innerEdges, withWait).map(_.headOption.getOrElse(false))
  }

  def addVertex(serviceName: String,
                columnName: String,
                id: Any,
                props: Map[String, Any] = Map.empty,
                ts: Long = System.currentTimeMillis(),
                 operation: String = "insert",
                 withWait: Boolean = true): Future[Boolean] = {
    val innerVertices = Seq(S2Vertex(this, serviceName, columnName, id, props.toMap, ts, operation).vertex)
    storage.mutateVertices(innerVertices, withWait).map(_.headOption.getOrElse(false))
  }

  def shutdown(): Unit = {
    storage.flush()
    Model.shutdown()
  }
}
