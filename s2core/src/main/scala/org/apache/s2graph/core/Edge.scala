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

import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.mysqls.{Label, LabelIndex, LabelMeta}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.logger
import play.api.libs.json.{JsNumber, Json}

import scala.collection.JavaConversions._


case class IndexEdge(srcVertex: Vertex,
                     tgtVertex: Vertex,
                     labelWithDir: LabelWithDirection,
                     op: Byte,
                     version: Long,
                     labelIndexSeq: Byte,
                     props: Map[Byte, InnerValLike]) {
  if (!props.containsKey(LabelMeta.timeStampSeq)) throw new Exception("Timestamp is required.")
  //  assert(props.containsKey(LabelMeta.timeStampSeq))

  val ts = props(LabelMeta.timeStampSeq).toString.toLong
  val degreeEdge = props.contains(LabelMeta.degreeSeq)
  lazy val label = Label.findById(labelWithDir.labelId)
  val schemaVer = label.schemaVersion
  lazy val labelIndex = LabelIndex.findByLabelIdAndSeq(labelWithDir.labelId, labelIndexSeq).get
  lazy val defaultIndexMetas = labelIndex.sortKeyTypes.map { meta =>
    val innerVal = toInnerVal(meta.defaultValue, meta.dataType, schemaVer)
    meta.seq -> innerVal
  }.toMap

  lazy val labelIndexMetaSeqs = labelIndex.metaSeqs

  /** TODO: make sure call of this class fill props as this assumes */
  lazy val orders = for (k <- labelIndexMetaSeqs if k >= 0) yield {
    props.get(k) match {
      case None =>

        /**
          * TODO: ugly hack
          * now we double store target vertex.innerId/srcVertex.innerId for easy development. later fix this to only store id once
          */
        val v = k match {
          case LabelMeta.timeStampSeq => InnerVal.withLong(version, schemaVer)
          case LabelMeta.toSeq => tgtVertex.innerId
          case LabelMeta.fromSeq => //srcVertex.innerId
            // for now, it does not make sense to build index on srcVertex.innerId since all edges have same data.
            throw new RuntimeException("_from on indexProps is not supported")
          case _ => defaultIndexMetas(k)
        }

        k -> v
      case Some(v) => k -> v
    }
  }

  lazy val ordersKeyMap = orders.map { case (byte, _) => byte }.toSet
  lazy val metas = for ((k, v) <- props if !ordersKeyMap.contains(k)) yield k -> v

  lazy val propsWithTs = props.map { case (k, v) => k -> InnerValLikeWithTs(v, version) }

  //TODO:
  //  lazy val kvs = Graph.client.indexedEdgeSerializer(this).toKeyValues.toList

  lazy val hasAllPropsForIndex = orders.length == labelIndexMetaSeqs.length

  def propsWithName = for {
    (seq, v) <- props
    meta <- label.metaPropsMap.get(seq) if seq >= 0
    jsValue <- innerValToJsValue(v, meta.dataType)
  } yield meta.name -> jsValue


  def toEdge(graph: Graph, label: Label): Edge = Edge(graph, label, this)

  // only for debug
  def toLogString() = {
    List(version, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, label.label, Json.toJson(propsWithName)).mkString("\t")
  }
}


case class SnapshotEdge(srcVertex: Vertex,
                        tgtVertex: Vertex,
                        labelWithDir: LabelWithDirection,
                        op: Byte,
                        version: Long,
                        props: Map[Byte, InnerValLikeWithTs],
                        pendingEdgeOpt: Option[SnapshotEdge],
                        statusCode: Byte = 0,
                        lockTs: Option[Long]) {

  val propsWithTs = props
  if (!props.containsKey(LabelMeta.timeStampSeq)) throw new Exception("Timestamp is required.")

  val label = Label.findById(labelWithDir.labelId)
  val schemaVer = label.schemaVersion
  lazy val propsWithoutTs = props.mapValues(_.innerVal)
  val ts = props(LabelMeta.timeStampSeq).innerVal.toString().toLong

  def toEdge(graph: Graph, label: Label): Edge = {
    Edge(graph, label, this)
  }

  def propsWithName = (for {
    (seq, v) <- props
    meta <- label.metaPropsMap.get(seq)
    jsValue <- innerValToJsValue(v.innerVal, meta.dataType)
  } yield meta.name -> jsValue) ++ Map("version" -> JsNumber(version))

  // only for debug
  def toLogString() = {
    List(ts, GraphUtil.fromOp(op), "e", srcVertex.innerId, tgtVertex.innerId, label.label, propsWithName).mkString("\t")
  }
}

case class Edge(graph: Graph,
                srcId: Any,
                tgtId: Any,
                label: Label,
                direction: Int,
                properties: Map[String, Any],
                op: Byte = GraphUtil.operations("insert"),
                version: Long = System.currentTimeMillis(),
                parentEdges: Seq[EdgeWithScore] = Nil,
                originalEdgeOpt: Option[Edge] = None,
                pendingEdgeOpt: Option[SnapshotEdge] = None,
                statusCode: Byte = 0,
                lockTs: Option[Long] = None) extends GraphElement {

  override def serviceName: String = label.serviceName

  /** only used for wal log publish */
  override def isAsync: Boolean = label.isAsync

  override def queueKey: String = Seq(ts.toString, label.serviceName).mkString("|")

  override def queuePartitionKey: String = Seq(srcId, tgtId).mkString("|")

  /** specify which serialize schema this edge should use */
  val schemaVer = label.schemaVersion

  override val ts = (properties.get(LabelMeta.timestamp.name), properties.get(LabelMeta.timestamp.name.drop(1))) match {
    case (Some(l: Long), _) => l
    case (_, Some(l: Long)) => l
    case (None, None) => throw new RuntimeException("Timestamp is required.")
    case _ => throw new RuntimeException("wrong type for timestamp.")
  }

  /** swap srcId and tgtId */
  def reverseSrcTgtEdge = copy(srcId = tgtId, tgtId = srcId)

  /** toggle direction */
  def reverseDirEdge = copy(direction = GraphUtil.toggleDir(direction))

  /** swap srcId and tgtId and toggle direction */
  def duplicateEdge = reverseSrcTgtEdge.reverseDirEdge

  /** find out related edges logically. */
  def relatedEdges =
    if (label.isDirected) List(this, duplicateEdge)
    else {
      /** with undirected graph, we only store 2 related edges */
      val base = copy(direction = GraphUtil.directions("out"))
      List(base, base.reverseSrcTgtEdge)
    }

  /** find out what indices are set on this label. */
  private def labelOrders = LabelIndex.findByLabelIdAll(label.id.get)

  def isDegree = properties.contains(LabelMeta.degree.name)

  /** find out necessary indexEdges for this label. */
  def edgesWithIndex = for (labelIndex <- labelOrders) yield {
    IndexEdge(this, labelIndex.seq)
  }


  def toSnapshotEdge: SnapshotEdge = SnapshotEdge(this)



  /**
   * yield rank score for one edge based on it's property and weights per property key
   * @param rankParam
   * @return
   */
  def rank(rankParam: RankParam): Double = {
    if (rankParam.propertyKeyWeights.isEmpty) 1.0f
    else {
      rankParam.propertyKeyWeights.foldLeft(0.0) { case (prev, (propertyKey, weight)) =>
        properties.get(propertyKey) match {
          case None => prev
          case Some(value) =>
            val cost = try value.toString.toDouble catch {
              case e: Exception =>
                logger.error(s"$propertyKey can't be used in scoring. ")
                1.0
            }
            prev + cost * weight
        }
      }
    }
  }

  def toLogString: String = {
    val ret =
      if (properties.nonEmpty)
        List(ts, op, "e", srcId, tgtId, label.label, Json.toJson(propertiesToJson(properties)))
      else
        List(ts, op, "e", srcId, tgtId, label.label, Json.obj())

    ret.mkString("\t")
  }
}

case class EdgeMutate(edgesToDelete: List[IndexEdge] = List.empty[IndexEdge],
                      edgesToInsert: List[IndexEdge] = List.empty[IndexEdge],
                      newSnapshotEdge: Option[SnapshotEdge] = None) {

  def toLogString: String = {
    val l = (0 until 50).map(_ => "-").mkString("")
    val deletes = s"deletes: ${edgesToDelete.map(e => e.toLogString).mkString("\n")}"
    val inserts = s"inserts: ${edgesToInsert.map(e => e.toLogString).mkString("\n")}"
    val updates = s"snapshot: ${newSnapshotEdge.map(e => e.toLogString).mkString("\n")}"

    List("\n", l, deletes, inserts, updates, l, "\n").mkString("\n")
  }
}

object SnapshotEdge {
  def apply(edge: Edge): SnapshotEdge = {
    val srcId = edge.srcId
    val tgtId = edge.tgtId
    val label = edge.label
    val srcVertexId = toInnerVal(srcId.toString, label.srcColumn.columnType, label.schemaVersion)
    val tgtVertexId = toInnerVal(tgtId.toString, label.tgtColumn.columnType, label.schemaVersion)

    val srcColId = label.srcColumn.id.get
    val tgtColId = label.tgtColumn.id.get

    val belongLabelIds = Seq(label.id.get)

    val srcVertex = Vertex(SourceVertexId(srcColId, srcVertexId), System.currentTimeMillis(), belongLabelIds = belongLabelIds)
    val tgtVertex = Vertex(TargetVertexId(tgtColId, tgtVertexId), System.currentTimeMillis(), belongLabelIds = belongLabelIds)

    val labelWithDir = LabelWithDirection(label.id.get, GraphUtil.directions("out"))

    val (src, tgt) =
      if (labelWithDir.dir == GraphUtil.directions("in")) (tgtVertex, srcVertex)
      else (srcVertex, tgtVertex)

    val props = label.toPropertiesWithTs(edge.properties, ts = edge.ts)
    SnapshotEdge(src, tgt, labelWithDir, edge.op, edge.version, props,
      pendingEdgeOpt = edge.pendingEdgeOpt, statusCode = edge.statusCode, lockTs = edge.lockTs)
  }
}
object IndexEdge {
  def apply(edge: Edge, labelIndexSeq: Byte = LabelIndex.DefaultSeq): IndexEdge = {
    val srcId = edge.srcId
    val tgtId = edge.tgtId
    val label = edge.label
    val srcVertexId = toInnerVal(srcId.toString, label.srcColumn.columnType, label.schemaVersion)
    val tgtVertexId = toInnerVal(tgtId.toString, label.tgtColumn.columnType, label.schemaVersion)

    val srcColId = label.srcColumn.id.get
    val tgtColId = label.tgtColumn.id.get

    val srcVertex = Vertex(SourceVertexId(srcColId, srcVertexId), System.currentTimeMillis())
    val tgtVertex = Vertex(TargetVertexId(tgtColId, tgtVertexId), System.currentTimeMillis())
    val labelWithDir = LabelWithDirection(label.id.get, edge.direction)

    val props = label.toProperties(edge.properties)
    IndexEdge(srcVertex, tgtVertex, labelWithDir, edge.op, edge.version, labelIndexSeq, props)
  }
}
object Edge {
  val incrementVersion = 1L
  val minTsVal = 0L

  def apply(graph: Graph, label: Label, snapshotEdge: SnapshotEdge): Edge =
    Edge(graph,
      snapshotEdge.srcVertex.innerId.value,
      snapshotEdge.tgtVertex.innerId.value,
      label,
      snapshotEdge.labelWithDir.dir,
      label.fromPropertiesWithTs(snapshotEdge.props),
      snapshotEdge.op,
      snapshotEdge.version,
      pendingEdgeOpt = snapshotEdge.pendingEdgeOpt,
      statusCode = snapshotEdge.statusCode,
      lockTs = snapshotEdge.lockTs)


  def apply(graph: Graph, label: Label, indexEdge: IndexEdge): Edge =
    Edge(graph,
      indexEdge.srcVertex.innerId.value,
      indexEdge.tgtVertex.innerId.value,
      label,
      indexEdge.labelWithDir.dir,
      label.fromProperties(indexEdge.props),
      indexEdge.op,
      indexEdge.version)

  /** now version information is required also **/
  type State = Map[Byte, InnerValLikeWithTs]
  type PropsPairWithTs = (State, State, Long, String)
  type MergeState = PropsPairWithTs => (State, Boolean)
  type UpdateFunc = (Option[Edge], Edge, MergeState)

  def allPropsDeleted(props: Map[Byte, InnerValLikeWithTs]): Boolean =
    if (!props.containsKey(LabelMeta.lastDeletedAt)) false
    else {
      val lastDeletedAt = props.get(LabelMeta.lastDeletedAt).get.ts
      val propsWithoutLastDeletedAt = props - LabelMeta.lastDeletedAt

      propsWithoutLastDeletedAt.forall { case (_, v) => v.ts <= lastDeletedAt }
    }

  def buildDeleteBulk(invertedEdge: Option[Edge], requestEdge: Edge): (Edge, EdgeMutate) = {
    //    assert(invertedEdge.isEmpty)
    //    assert(requestEdge.op == GraphUtil.operations("delete"))

    val edgesToDelete = requestEdge.relatedEdges.flatMap { relEdge => relEdge.edgesWithIndex }
    val edgeInverted = Option(requestEdge.toSnapshotEdge)

    (requestEdge, EdgeMutate(edgesToDelete, edgesToInsert = Nil, edgeInverted))
  }






  def buildOperation(graph: Graph, label: Label, snapshotEdge: Option[SnapshotEdge], requestEdges: Seq[Edge]): (Edge, EdgeMutate) = {
    //            logger.debug(s"oldEdge: ${invertedEdge.map(_.toStringRaw)}")
    //            logger.debug(s"requestEdge: ${requestEdge.toStringRaw}")
    val oldPropsWithTs =
      if (snapshotEdge.isEmpty) Map.empty[Byte, InnerValLikeWithTs] else snapshotEdge.get.propsWithTs

    val funcs = requestEdges.map { edge =>
      if (edge.op == GraphUtil.operations("insert")) {
        edge.label.consistencyLevel match {
          case "strong" => Edge.mergeUpsert _
          case _ => Edge.mergeInsertBulk _
        }
      } else if (edge.op == GraphUtil.operations("insertBulk")) {
        Edge.mergeInsertBulk _
      } else if (edge.op == GraphUtil.operations("delete")) {
        edge.label.consistencyLevel match {
          case "strong" => Edge.mergeDelete _
          case _ => throw new RuntimeException("not supported")
        }
      }
      else if (edge.op == GraphUtil.operations("update")) Edge.mergeUpdate _
      else if (edge.op == GraphUtil.operations("increment")) Edge.mergeIncrement _
      else throw new RuntimeException(s"not supported operation on edge: $edge")
    }

    val oldTs = snapshotEdge.map(_.ts).getOrElse(minTsVal)
    val requestWithFuncs = requestEdges.zip(funcs).filter(oldTs != _._1.ts).sortBy(_._1.ts)

    if (requestWithFuncs.isEmpty) {
      (requestEdges.head, EdgeMutate())
    } else {
      val requestEdge = requestWithFuncs.last._1
      var prevPropsWithTs = oldPropsWithTs

      for {
        (requestEdge, func) <- requestWithFuncs
      } {
        val (_newPropsWithTs, _) = func(prevPropsWithTs, requestEdge.toSnapshotEdge.propsWithTs, requestEdge.ts, requestEdge.schemaVer)
        prevPropsWithTs = _newPropsWithTs
        //        logger.debug(s"${requestEdge.toLogString}\n$oldPropsWithTs\n$prevPropsWithTs\n")
      }
      val requestTs = requestEdge.ts
      /** version should be monotoniously increasing so our RPC mutation should be applied safely */
      val newVersion = snapshotEdge.map(e => e.version + incrementVersion).getOrElse(requestTs)
      val maxTs = prevPropsWithTs.map(_._2.ts).max
      val newTs = if (maxTs > requestTs) maxTs else requestTs
      val propsWithTs = prevPropsWithTs ++
        Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(newTs, requestEdge.label.schemaVersion), newTs))
      val edgeMutate = buildMutation(graph, label, snapshotEdge, requestEdge, newVersion, oldPropsWithTs, propsWithTs)

      //      logger.debug(s"${edgeMutate.toLogString}\n${propsWithTs}")
      //      logger.error(s"$propsWithTs")
      (requestEdge, edgeMutate)
    }
  }

  //TODO: transform from Edge -> SnapshotEdge, SnapshotEdge -> Edge need to be resorted.
  def buildMutation(graph: Graph,
                    label: Label,
                    snapshotEdgeOpt: Option[SnapshotEdge],
                    requestEdge: Edge,
                    newVersion: Long,
                    oldPropsWithTs: Map[Byte, InnerValLikeWithTs],
                    newPropsWithTs: Map[Byte, InnerValLikeWithTs]): EdgeMutate = {
    if (oldPropsWithTs == newPropsWithTs) {
      // all requests should be dropped. so empty mutation.
      //      logger.error(s"Case 1")
      EdgeMutate(edgesToDelete = Nil, edgesToInsert = Nil, newSnapshotEdge = None)
    } else {
      val withOutDeletedAt = newPropsWithTs.filter(kv => kv._1 != LabelMeta.lastDeletedAt)
      val newOp = snapshotEdgeOpt match {
        case None => requestEdge.op
        case Some(old) =>
          val oldMaxTs = old.propsWithTs.map(_._2.ts).max
          if (oldMaxTs > requestEdge.ts) old.op
          else requestEdge.op
      }

      val newSnapshotEdgeOpt =
        Option(requestEdge.toSnapshotEdge.copy(op = newOp, props = newPropsWithTs, version = newVersion))
      // delete request must always update snapshot.
      if (withOutDeletedAt == oldPropsWithTs && newPropsWithTs.containsKey(LabelMeta.lastDeletedAt)) {
        // no mutation on indexEdges. only snapshotEdge should be updated to record lastDeletedAt.
        //        logger.error(s"Case 2")
        EdgeMutate(edgesToDelete = Nil, edgesToInsert = Nil, newSnapshotEdge = newSnapshotEdgeOpt)
      } else {
        //        logger.error(s"Case 3")
        val edgesToDelete = snapshotEdgeOpt match {
          case Some(snapshotEdge) if snapshotEdge.op != GraphUtil.operations("delete") =>
            snapshotEdge.copy(op = GraphUtil.defaultOpByte).toEdge(graph, label).
              relatedEdges.flatMap { relEdge => relEdge.edgesWithIndex }
          case _ => Nil
        }

        val edgesToInsert =
          if (newPropsWithTs.isEmpty || allPropsDeleted(newPropsWithTs)) Nil
          else
            requestEdge.toSnapshotEdge.copy(version = newVersion, props = newPropsWithTs, op = GraphUtil.defaultOpByte).toEdge(graph, label)
              .relatedEdges.flatMap { relEdge => relEdge.edgesWithIndex }

        EdgeMutate(edgesToDelete = edgesToDelete, edgesToInsert = edgesToInsert, newSnapshotEdge = newSnapshotEdgeOpt)
      }
    }
  }

  def mergeUpsert(propsPairWithTs: PropsPairWithTs): (State, Boolean) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs, version) = propsPairWithTs
    val lastDeletedAt = oldPropsWithTs.get(LabelMeta.lastDeletedAt).map(v => v.ts).getOrElse(minTsVal)
    val existInOld = for ((k, oldValWithTs) <- oldPropsWithTs) yield {
      propsWithTs.get(k) match {
        case Some(newValWithTs) =>
          assert(oldValWithTs.ts >= lastDeletedAt)
          val v = if (oldValWithTs.ts >= newValWithTs.ts) oldValWithTs
          else {
            shouldReplace = true
            newValWithTs
          }
          Some(k -> v)

        case None =>
          assert(oldValWithTs.ts >= lastDeletedAt)
          if (oldValWithTs.ts >= requestTs || k < 0) Some(k -> oldValWithTs)
          else {
            shouldReplace = true
            None
          }
      }
    }
    val existInNew =
      for {
        (k, newValWithTs) <- propsWithTs if !oldPropsWithTs.contains(k) && newValWithTs.ts > lastDeletedAt
      } yield {
        shouldReplace = true
        Some(k -> newValWithTs)
      }

    ((existInOld.flatten ++ existInNew.flatten).toMap, shouldReplace)
  }

  def mergeUpdate(propsPairWithTs: PropsPairWithTs): (State, Boolean) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs, version) = propsPairWithTs
    val lastDeletedAt = oldPropsWithTs.get(LabelMeta.lastDeletedAt).map(v => v.ts).getOrElse(minTsVal)
    val existInOld = for ((k, oldValWithTs) <- oldPropsWithTs) yield {
      propsWithTs.get(k) match {
        case Some(newValWithTs) =>
          assert(oldValWithTs.ts >= lastDeletedAt)
          val v = if (oldValWithTs.ts >= newValWithTs.ts) oldValWithTs
          else {
            shouldReplace = true
            newValWithTs
          }
          Some(k -> v)
        case None =>
          // important: update need to merge previous valid values.
          assert(oldValWithTs.ts >= lastDeletedAt)
          Some(k -> oldValWithTs)
      }
    }
    val existInNew = for {
      (k, newValWithTs) <- propsWithTs if !oldPropsWithTs.contains(k) && newValWithTs.ts > lastDeletedAt
    } yield {
      shouldReplace = true
      Some(k -> newValWithTs)
    }

    ((existInOld.flatten ++ existInNew.flatten).toMap, shouldReplace)
  }

  def mergeIncrement(propsPairWithTs: PropsPairWithTs): (State, Boolean) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs, version) = propsPairWithTs
    val lastDeletedAt = oldPropsWithTs.get(LabelMeta.lastDeletedAt).map(v => v.ts).getOrElse(minTsVal)
    val existInOld = for ((k, oldValWithTs) <- oldPropsWithTs) yield {
      propsWithTs.get(k) match {
        case Some(newValWithTs) =>
          if (k == LabelMeta.timeStampSeq) {
            val v = if (oldValWithTs.ts >= newValWithTs.ts) oldValWithTs
            else {
              shouldReplace = true
              newValWithTs
            }
            Some(k -> v)
          } else {
            if (oldValWithTs.ts >= newValWithTs.ts) {
              Some(k -> oldValWithTs)
            } else {
              assert(oldValWithTs.ts < newValWithTs.ts && oldValWithTs.ts >= lastDeletedAt)
              shouldReplace = true
              // incr(t0), incr(t2), d(t1) => deleted
              Some(k -> InnerValLikeWithTs(oldValWithTs.innerVal + newValWithTs.innerVal, oldValWithTs.ts))
            }
          }

        case None =>
          assert(oldValWithTs.ts >= lastDeletedAt)
          Some(k -> oldValWithTs)
        //          if (oldValWithTs.ts >= lastDeletedAt) Some(k -> oldValWithTs) else None
      }
    }
    val existInNew = for {
      (k, newValWithTs) <- propsWithTs if !oldPropsWithTs.contains(k) && newValWithTs.ts > lastDeletedAt
    } yield {
      shouldReplace = true
      Some(k -> newValWithTs)
    }

    ((existInOld.flatten ++ existInNew.flatten).toMap, shouldReplace)
  }

  def mergeDelete(propsPairWithTs: PropsPairWithTs): (State, Boolean) = {
    var shouldReplace = false
    val (oldPropsWithTs, propsWithTs, requestTs, version) = propsPairWithTs
    val lastDeletedAt = oldPropsWithTs.get(LabelMeta.lastDeletedAt) match {
      case Some(prevDeletedAt) =>
        if (prevDeletedAt.ts >= requestTs) prevDeletedAt.ts
        else {
          shouldReplace = true
          requestTs
        }
      case None => {
        shouldReplace = true
        requestTs
      }
    }
    val existInOld = for ((k, oldValWithTs) <- oldPropsWithTs) yield {
      if (k == LabelMeta.timeStampSeq) {
        if (oldValWithTs.ts >= requestTs) Some(k -> oldValWithTs)
        else {
          shouldReplace = true
          Some(k -> InnerValLikeWithTs.withLong(requestTs, requestTs, version))
        }
      } else {
        if (oldValWithTs.ts >= lastDeletedAt) Some(k -> oldValWithTs)
        else {
          shouldReplace = true
          None
        }
      }
    }
    val mustExistInNew = Map(LabelMeta.lastDeletedAt -> InnerValLikeWithTs.withLong(lastDeletedAt, lastDeletedAt, version))
    ((existInOld.flatten ++ mustExistInNew).toMap, shouldReplace)
  }

  def mergeInsertBulk(propsPairWithTs: PropsPairWithTs): (State, Boolean) = {
    val (_, propsWithTs, _, _) = propsPairWithTs
    (propsWithTs, true)
  }

//  def fromString(s: String): Option[Edge] = Graph.toEdge(s)


}
