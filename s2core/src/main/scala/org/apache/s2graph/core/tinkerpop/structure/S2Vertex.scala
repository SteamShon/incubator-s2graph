package org.apache.s2graph.core.tinkerpop.structure


import org.apache.s2graph._
import org.apache.s2graph.core.mysqls.Label
import org.apache.s2graph.core.{GraphUtil, Management}
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure._
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.collection.JavaConversions._
import java.util
import java.util.concurrent.TimeUnit


class S2Vertex(val graph: S2Graph,
               val vertex: core.Vertex,
               val label: String) extends Vertex {
  val properties = new mutable.HashMap[String, Seq[VertexProperty[_]]]

  override def vertices(direction: Direction, strings: String*): util.Iterator[Vertex] = ???

  override def edges(direction: Direction, strings: String*): util.Iterator[Edge] = ???

  override def property[V](cardinality: Cardinality,
                           key: String,
                           value: V,
                           kvs: AnyRef*): VertexProperty[V] = ???
//  {
//    val vertexProperty = new S2VertexProperty[V](id(), this, key, value)
//    val ls = properties.get(key) match {
//      case None => Seq(vertexProperty)
//      case Some(old) => old :+ vertexProperty
//    }
//    properties.put(key, ls)
//    ElementHelper.attachProperties(vertexProperty, kvs)
//    vertexProperty
//  }


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
      if (rets.forall(identity)) graph.toS2Edge(edge, labelName)
      else throw new RuntimeException("addEdge to storage failed.")
    }(graph.ec)
    Await.result(future, Duration(graph.WriteRPCTimeOut, TimeUnit.MILLISECONDS))
  }

  override def properties[V](keys: String*): util.Iterator[VertexProperty[V]] = ???
//  {
//    val set = for {
//      key <- keys
//      value = property[V](key) if value != null
//    } yield value
//
//    set.iterator
//  }

  override def remove(): Unit = ???

  override def id(): AnyRef = vertex.id
}
