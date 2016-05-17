package org.apache.s2graph.core.tinkerpop.structure


import org.apache.s2graph._
import org.apache.s2graph.core.mysqls.Label
import org.apache.s2graph.core._
import org.apache.s2graph.core.types.LabelWithDirection
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure._
import scala.concurrent.Await
import java.util
import scala.collection.JavaConversions._

class S2Vertex(val graph: S2Graph,
               val vertex: core.Vertex,
               val label: String) extends Vertex {

  override def vertices(direction: Direction, labels: String*): util.Iterator[Vertex] = {
    val ls = this.edges(direction, labels: _*).flatMap { edges =>
      direction match {
        case Direction.OUT => Seq(edges.outVertex())
        case Direction.IN => Seq(edges.inVertex())
        case _ => Seq(edges.outVertex(), edges.inVertex())
      }
    }
    ls.toIterator
  }

  override def edges(direction: Direction, labels: String*): util.Iterator[Edge] = {
    val queryParams = labels.map { label =>
      Label.findByName(label).map(l => QueryParam(labelWithDir = LabelWithDirection(l.id.get, 0)))
    }.flatten

    val steps = Vector(Step(queryParams = queryParams.toList))
    val query = Query(vertices = Seq(vertex), steps = steps, queryOption = QueryOption())

    val future = graph.client.getEdges(query)
    val fetched = Await.result(future, graph.ReadRpcTimeout)
    val edges = fetched.flatMap { ls =>

      ls.queryResult.edgeWithScoreLs.map { edgeWithScore =>
        val edge = graph.toS2Edge(edgeWithScore.edge)
        edge
      }
    }
    edges.iterator
  }

  override def property[V](cardinality: Cardinality,
                           key: String,
                           value: V,
                           kvs: AnyRef*): VertexProperty[V] = ???


  override def addEdge(labelName: String, tgtVertex: Vertex, kvs: AnyRef*): Edge = {
    if (!tgtVertex.isInstanceOf[S2Vertex]) throw new RuntimeException("not supported type of vertex on tgtVertex.")
    val edgeProps = Management.toPropsJson(kvs)
    val tgtV = tgtVertex.asInstanceOf[S2Vertex]
    val srcId = vertex.innerId.toIdString()
    val tgtId = tgtV.vertex.innerId.toIdString()
    val ts = edgeProps.get("timestamp").map(_.toString.toLong).getOrElse(System.currentTimeMillis())
    val op = edgeProps.get("op").map(GraphUtil.jsValueToStr(_)).getOrElse("insert")
    val dir = "out"
    val mustHaveProps = Map("timestamp" -> ts,
      "op" -> op, "from" -> srcId, "to" -> tgtId, "direction" -> dir)
    /** FIX ME: am I right ? */
    val allEdgeProps = edgeProps ++ mustHaveProps.toSeq

    val edgeLabel = Label.findByName(labelName).getOrElse(throw new RuntimeException(s"can't find edge: $labelName"))
    val edge = Management.toEdgeWithLabel(edgeLabel)(allEdgeProps.toSeq: _*)

    val future = graph.client.mutateEdges(Seq(edge), withWait = true).map { rets =>
      if (rets.forall(identity)) graph.toS2Edge(edge)
      else throw new RuntimeException("addEdge to storage failed.")
    }(graph.ec)
    Await.result(future, graph.WriteRPCTimeOut)
  }

  override def properties[V](keys: String*): util.Iterator[VertexProperty[V]] = ???

  override def remove(): Unit = ???

  override def id(): AnyRef = vertex.id
}
