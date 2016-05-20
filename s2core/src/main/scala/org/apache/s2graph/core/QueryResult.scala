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
import org.apache.s2graph.core.utils.logger

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}

object QueryResult {
  def fromVertices(query: Query): StepInnerResult = {
    if (query.steps.isEmpty || query.steps.head.queryParams.isEmpty) {
      StepInnerResult.Empty
    } else {
      val queryParam = query.steps.head.queryParams.head
      val label = queryParam.label
      val currentTs = System.currentTimeMillis()
      val propsWithTs = Map(LabelMeta.timeStampSeq ->
        InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs))
      val edgeWithScoreLs = for {
        vertex <- query.vertices
      } yield {
        val edge = Edge(vertex, vertex, queryParam.labelWithDir, propsWithTs = propsWithTs)
        val edgeWithScore = EdgeWithScore(edge, Graph.DefaultScore)
        edgeWithScore
//        QueryRequestWithResult(QueryRequest(query, -1, vertex, queryParam),
//          QueryResult(edgeWithScoreLs = Seq(edgeWithScore)))
      }
      StepInnerResult(edgesWithScoreLs = edgeWithScoreLs, Nil, false)
    }
  }
}
/** inner traverse */
object StepInnerResult {
  val Failure = StepInnerResult(Nil, Nil, true)
  val Empty = StepInnerResult(Nil, Nil, false)
}
case class StepInnerResult(edgesWithScoreLs: Seq[EdgeWithScore],
                           degreeEdges: Seq[EdgeWithScore],
                           isFailure: Boolean = false) {
  val isEmpty = edgesWithScoreLs.isEmpty
}
//case class QueryRequestWithResult(queryRequest: QueryRequest,
//                                  queryResult: QueryResult)
//
case class QueryRequest(query: Query,
                        stepIdx: Int,
                        vertex: Vertex,
                        queryParam: QueryParam)
//
//
//case class QueryResult(edgeWithScoreLs: Seq[EdgeWithScore] = Nil,
//                       tailCursor: Array[Byte] = Array.empty,
//                       timestamp: Long = System.currentTimeMillis(),
//                       isFailure: Boolean = false)

case class EdgeWithScore(edge: Edge, score: Double)





/** result */

object StepResult {
  import OrderingUtil._

  type Values = Seq[S2EdgeWithScore]
  type GroupByKey = Seq[Option[Any]]
  val EmptyOrderByValues = (None, None, None, None)
  val Empty = StepResult(Nil, Nil, Nil)


  def mergeOrdered(left: StepResult.Values,
                   right: StepResult.Values,
                   queryOption: QueryOption): (Double, StepResult.Values) = {
    val merged = (left ++ right)
    val scoreSum = merged.foldLeft(0.0) { case (prev, current) => prev + current.score }
    if (scoreSum < queryOption.scoreThreshold) (0.0, Nil)
    else {
      val ordered = orderBy(queryOption, merged)
      val filtered = ordered.take(queryOption.groupedLimit)
      val newScoreSum = filtered.foldLeft(0.0) { case (prev, current) => prev + current.score }
      (newScoreSum, filtered)
    }
//    val leftLen = left.size
//    val rightLen = right.size
//    val resultLen = if (leftLen + rightLen >= stopLimit) leftLen + rightLen else stopLimit
//    val buffer = new Array[S2EdgeWithScore](resultLen)
//    var scoreSum = 0.0
//    var i = 0
//    var j = 0
//    var k = 0
//    var shouldStop = false
//    while (!shouldStop && i < leftLen && j < rightLen && k < resultLen) {
//      if (k >= stopLimit)
//        if (left(i).score > right(j).score) {
//          if (left(i).score < stopScore) shouldStop = true
//          else {
//            buffer(k) = left(i)
//            scoreSum += buffer(k).score
//            i += 1
//            k += 1
//          }
//        } else {
//          if (right(j).score < stopScore) shouldStop = true
//          else {
//            buffer(k) = right(j)
//            scoreSum += buffer(k).score
//            j += 1
//            k += 1
//          }
//        }
//    }
//
//    logger.debug(s"[MergeOrdered.Left]: $left")
//    logger.debug(s"[MergeOrdered.Right]: $right")
//    logger.debug(s"[MergeOrdered.StopScore]: $stopScore")
//    logger.debug(s"[MergeOrdered.StopLimit]: $stopLimit")
//    logger.debug(s"[MergeOrdered]: ${buffer.mkString("\n")}")
//    (scoreSum, buffer.take(k))
  }

//  /**
//   *
//   * @param queryOption
//   * @param base
//   * @param lookup
//   * @param agg
//   */
//  def mergeIntoMutable(queryOption: QueryOption,
//                       base: Seq[(GroupByKey, (Double, Values))],
//                       lookup: Map[GroupByKey, (Double, Values)],
//                       agg: mutable.HashMap[GroupByKey, (Double, Values)]): Unit = {
//    for {
//      (groupByKey, (scoreSum, edges)) <- base
//    } {
//      lookup.get(groupByKey) match {
//        case None => // only exit on base.
//          agg += ((groupByKey, (scoreSum -> edges)))
//        case Some((_, otherEdges)) => // both exist.
//          val (newScoreSum, merged) = mergeOrdered(edges, otherEdges, queryOption.scoreThreshold, queryOption.groupedLimit)
//          agg += ((groupByKey, (newScoreSum, merged)))
//      }
//    }
//  }


//  /**
//   * merge two result into one. this is necessary for multiquery.
//   * note that each result(baseStepResult, otherStepResult) are ordered(flatten) or
//   * grouped and ordered(groupBy).
//   *
//   * TODO: need to apply duplicate policy on step, multiquery.
//   * @param queryOption
//   * @param baseStepResult
//   * @param otherStepResult
//   * @return
//   */
//  def merge(queryOption: QueryOption,
//            baseStepResult: StepResult,
//            otherStepResult: StepResult): StepResult = {
//    val degrees = baseStepResult.degreeEdges ++ otherStepResult.degreeEdges
//    val (_, ordered) = mergeOrdered(baseStepResult.results,
//      otherStepResult.results, queryOption.scoreThreshold, queryOption.limit)
//
//
//    if (queryOption.groupByColumns.isEmpty) StepResult(results = ordered, grouped = Nil, Nil, degrees)
//    else {
//      //need to process grouped.
//      val builder = new mutable.HashMap[GroupByKey, (Double, Values)]()
//      val base = baseStepResult.grouped.toMap
//      val other = otherStepResult.grouped.toMap
//
//      // first start with base.
//      mergeIntoMutable(queryOption, baseStepResult.grouped, other, builder)
//      // second go through other result.
//      mergeIntoMutable(queryOption, otherStepResult.grouped, base, builder)
//
//      val grouped = builder.toSeq.sortBy(_._2._1)
//      StepResult(results = ordered, grouped = grouped, queryRequestWithResultLs = Nil, degreeEdges = degrees)
//    }
//  }


  def orderBy(queryOption: QueryOption, notOrdered: Values): Values = {
    import OrderingUtil._

    if (queryOption.withScore && queryOption.orderByColumns.nonEmpty) {
      notOrdered.sortBy(_.orderByValues)(TupleMultiOrdering[Any](queryOption.ascendingVals))
    } else {
      notOrdered
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
  /**
   * merge multiple StepResult into one StepResult.
   * @param queryOption
   * @param multiStepResults
   * @return
   */
  def merges(queryOption: QueryOption,
             multiStepResults: Seq[StepResult],
             weights: Seq[Double] = Nil): StepResult = {
    val degrees = multiStepResults.flatMap(_.degreeEdges)
    val ls = new mutable.ListBuffer[S2EdgeWithScore]()
    val agg= new mutable.HashMap[GroupByKey, ListBuffer[S2EdgeWithScore]]()
    val sums = new mutable.HashMap[GroupByKey, Double]()

    for {
      (weight, eachStepResult) <- weights.zip(multiStepResults)
      (ordered, grouped) = (eachStepResult.results, eachStepResult.grouped)
    } {
      ordered.foreach { t =>
        val newScore = t.score * weight
        ls += t.copy(score = newScore)
      }

      // process each query's stepResult's grouped
      for {
        (groupByKey, (scoreSum, values)) <- grouped
      } {
        val buffer = agg.getOrElseUpdate(groupByKey, ListBuffer.empty[S2EdgeWithScore])
        var scoreSum = 0.0
        values.foreach { t =>
          val newScore = t.score * weight
          buffer += t.copy(score = newScore)
          scoreSum += newScore
        }
        sums += (groupByKey -> scoreSum)
      }
    }

    // process global groupBy
    if (queryOption.groupByColumns.nonEmpty) {
      for {
        s2EdgeWithScore <- ls
        groupByKey = s2EdgeWithScore.s2Edge.selectValues(queryOption.groupByColumns)
      } {
        val buffer = agg.getOrElseUpdate(groupByKey, ListBuffer.empty[S2EdgeWithScore])
        buffer += s2EdgeWithScore
        val newScore = sums.getOrElse(groupByKey, 0.0) + s2EdgeWithScore.score
        sums += (groupByKey -> newScore)
      }
    }


    val ordered = orderBy(queryOption, ls)
    val grouped = for {
      (groupByKey, scoreSum) <- sums.toSeq.sortBy(_._2 * -1)
      aggregated = agg(groupByKey) if aggregated.nonEmpty
    } yield groupByKey -> (scoreSum, aggregated)

    StepResult(results = ordered, grouped = grouped, degrees)
  }
  def filterOut(queryOption: QueryOption,
                baseStepResult: StepResult,
                filterOutStepResult: StepResult): StepResult = {
    //TODO: Optimize this.
    val fields = if (queryOption.filterOutFields.isEmpty) Seq("to") else Seq("to")
    //    else queryOption.filterOutFields
    val filterOutSet = filterOutStepResult.results.map { t =>
      t.s2Edge.selectValues(fields)
    }.toSet

    val filteredResults = baseStepResult.results.filter { t =>
      val filterOutKey = t.s2Edge.selectValues(fields)
      !filterOutSet.contains(filterOutKey)
    }

    val grouped = for {
      (key, (scoreSum, values)) <- baseStepResult.grouped
      (out, in) = values.partition(v => filterOutSet.contains(v.s2Edge.selectValues(fields)))
      newScoreSum = scoreSum - out.foldLeft(0.0) { case (prev, current) => prev + current.score } if in.nonEmpty
    } yield key -> (newScoreSum, in)


    StepResult(results = filteredResults, grouped = grouped, baseStepResult.degreeEdges)
  }

  def apply(graph: Graph,
            queryOption: QueryOption,
            stepInnerResult: StepInnerResult): StepResult = {
    stepInnerResult.edgesWithScoreLs.foreach(t => logger.debug(s"[AA]: $t"))

    val results = for {
      edgeWithScore <- stepInnerResult.edgesWithScoreLs
    } yield {
        val s2Edge = S2Edge.apply(graph, edgeWithScore.edge)
        val orderByValues =
          if (queryOption.orderByColumns.isEmpty) (edgeWithScore.score, None, None, None)
          else toOrderByValues(s2Edge, edgeWithScore.score, queryOption.orderByKeys)

        S2EdgeWithScore(s2Edge, edgeWithScore.score, orderByValues, edgeWithScore.edge.parentEdges)
      }
    /** ordered flatten result */
    val ordered = orderBy(queryOption, results)

    /** ordered grouped result */
    val grouped =
      if (queryOption.groupByColumns.isEmpty) Nil
      else {
        val agg = new mutable.HashMap[GroupByKey, (Double, Values)]()
        results.groupBy { s2EdgeWithScore =>
          s2EdgeWithScore.s2Edge.selectValues(queryOption.groupByColumns, useToString = true)
        }.map { case (k, ls) =>
          val (scoreSum, merged) = mergeOrdered(ls, Nil, queryOption)
          /**
           * watch out here. by calling toString on Any, we lose type information which will be used
           * later for toJson.
           */
          if (merged.nonEmpty) {
            val newKey = merged.head.s2Edge.selectValues(queryOption.groupByColumns, useToString = false)
            agg += (newKey -> (scoreSum, merged))
          }
        }
        agg.toSeq.sortBy(_._2._1 * -1)
      }

    val degrees = stepInnerResult.degreeEdges.map(t => S2EdgeWithScore(S2Edge.apply(graph, t.edge), t.score))
    StepResult(results = ordered, grouped = grouped, degreeEdges = degrees)
  }
}

case class S2EdgeWithScore(s2Edge: S2Edge,
                           score: Double,
                           orderByValues: (Any, Any, Any, Any) = StepResult.EmptyOrderByValues,
                           parentEdges: Seq[EdgeWithScore] = Nil)

case class StepResult(results: StepResult.Values,
                      grouped: Seq[(StepResult.GroupByKey, (Double, StepResult.Values))],
                      degreeEdges: StepResult.Values)