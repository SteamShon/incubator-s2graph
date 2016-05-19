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

import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.types.{InnerVal, InnerValLikeWithTs}

import scala.collection.{mutable, Seq}
import scala.collection.mutable.ListBuffer

object QueryResult {
  def fromVertices(query: Query): Seq[QueryRequestWithResult] = {
    if (query.steps.isEmpty || query.steps.head.queryParams.isEmpty) {
      Seq.empty
    } else {
      val queryParam = query.steps.head.queryParams.head
      val label = queryParam.label
      val currentTs = System.currentTimeMillis()
      val propsWithTs = Map(LabelMeta.timeStampSeq ->
        InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs))
      for {
        vertex <- query.vertices
      } yield {
        val edge = Edge(vertex, vertex, queryParam.labelWithDir, propsWithTs = propsWithTs)
        val edgeWithScore = EdgeWithScore(edge, Graph.DefaultScore)
        QueryRequestWithResult(QueryRequest(query, -1, vertex, queryParam),
          QueryResult(edgeWithScoreLs = Seq(edgeWithScore)))
      }
    }
  }
}
/** inner traverse */
case class QueryRequestWithResult(queryRequest: QueryRequest,
                                  queryResult: QueryResult)

case class QueryRequest(query: Query,
                        stepIdx: Int,
                        vertex: Vertex,
                        queryParam: QueryParam)


case class QueryResult(edgeWithScoreLs: Seq[EdgeWithScore] = Nil,
                       tailCursor: Array[Byte] = Array.empty,
                       timestamp: Long = System.currentTimeMillis(),
                       isFailure: Boolean = false)

case class EdgeWithScore(edge: Edge, score: Double)





/** result */

object StepResult {
  import OrderingUtil._

  type Values = Seq[S2EdgeWithScore]
  type GroupByKey = Seq[Option[Any]]
  val EmptyOrderByValues = (None, None, None, None)
  val Empty = StepResult(Nil, Map.empty[GroupByKey, Values], Nil, Nil)

  /** TODO implement this. */
  def filterOut(baseStepResult: StepResult, filterOutStepResult: StepResult): StepResult = {
    val filterOutEdges = filterOutStepResult.results.map(_.s2Edge.uniqueId).toSet
    val filteredResults = baseStepResult.results.filter(t => !filterOutEdges.contains(t.s2Edge.uniqueId))

    val grouped = for {
      (key, values) <- baseStepResult.grouped
    } yield key -> values.filter(v => !filterOutEdges.contains(v.s2Edge.uniqueId))

    StepResult(results = filteredResults, grouped = grouped, queryRequestWithResultLs = Nil, baseStepResult.degreeEdges)
  }
  def merge(baseStepResult: StepResult, otherStepResult: StepResult): StepResult = {
    baseStepResult
  }

  def filterEdgeWithScoreLs(graph: Graph,
                            result: QueryResult,
                            degreeEdges: ListBuffer[S2EdgeWithScore]): Seq[EdgeWithScore] = {
    val head = result.edgeWithScoreLs.head
    if (head.edge.isDegree) {
      degreeEdges += S2EdgeWithScore(S2Edge(graph, head.edge), head.score)
      result.edgeWithScoreLs.tail
    } else {
      result.edgeWithScoreLs
    }
  }

  def toOrderByValues(s2Edge: S2Edge,
                       score: Double,
                       orderByKeys: Seq[String]): (Any, Any, Any, Any) = {
    def toValue(propertyKey: String): Any = {
      propertyKey match {
        case "score" => score
        case "timestamp" | "_timestamp" => s2Edge.ts
        case _ => s2Edge.props.get(propertyKey)
      }
    }
    if (orderByKeys.isEmpty) (None, None, None, None)
    else {
      orderByKeys.length match {
        case 1 =>
          (toValue(orderByKeys(0)), None, None, None)
        case 2 =>
          (toValue(orderByKeys(0)), toValue(orderByKeys(1)), None, None)
        case 3 =>
          (toValue(orderByKeys(0)), toValue(orderByKeys(1)), toValue(orderByKeys(2)), None)
        case _ =>
          (toValue(orderByKeys(0)), toValue(orderByKeys(1)), toValue(orderByKeys(2)), toValue(orderByKeys(3)))
      }
    }
  }


  //TODO: OrderBy, Select is not implemented.
  def apply(graph: Graph,
            query: Query,
            queryRequestWithResultLs: Seq[QueryRequestWithResult]): StepResult = {

    val degreeEdges = new ListBuffer[S2EdgeWithScore]()
    val results = for {
      requestWithResult <- queryRequestWithResultLs
      (request, result) = QueryRequestWithResult.unapply(requestWithResult).get if result.edgeWithScoreLs.nonEmpty
      edgeWithScore <- filterEdgeWithScoreLs(graph, result, degreeEdges)
    } yield {
        val s2Edge = S2Edge.apply(graph, edgeWithScore.edge)
        val orderByValues =
          if (query.orderByColumns.isEmpty) (edgeWithScore.score, None, None, None)
          else toOrderByValues(s2Edge, edgeWithScore.score, query.orderByKeys)
        S2EdgeWithScore(s2Edge, edgeWithScore.score, orderByValues)
      }
    val ordered = results.sortBy(_.orderByValues)(TupleMultiOrdering(query.ascendingVals))

    val grouped =
      if (query.groupByColumns.isEmpty) Map.empty[StepResult.GroupByKey, StepResult.Values]
      else {
        val groupedBy = results.groupBy { s2EdgeWithScore =>
          s2EdgeWithScore.s2Edge.toGroupByKey(query.groupByColumns)
        }
        groupedBy.map { case (k, ls) =>
//          val scoreSum = ls.foldLeft(0.0) { case (prev, current) => current.score }
          k -> ls.sortBy(_.orderByValues)(TupleMultiOrdering(query.ascendingVals))
        }
      }

    StepResult(results = ordered, grouped = grouped,
      queryRequestWithResultLs = queryRequestWithResultLs, degreeEdges)
  }
}

case class S2EdgeWithScore(s2Edge: S2Edge,
                           score: Double,
                           orderByValues: (Any, Any, Any, Any) = StepResult.EmptyOrderByValues)

case class StepResult(results: StepResult.Values,
                      grouped: Map[StepResult.GroupByKey, StepResult.Values],
                      queryRequestWithResultLs: Seq[QueryRequestWithResult],
                      degreeEdges: StepResult.Values)