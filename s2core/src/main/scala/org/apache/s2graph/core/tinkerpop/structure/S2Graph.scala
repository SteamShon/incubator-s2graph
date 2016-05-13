package org.apache.s2graph.core.tinkerpop.structure

import java.util
import java.util.concurrent.{TimeUnit, Executors}

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.commons.configuration.{BaseConfiguration, Configuration}
import org.apache.s2graph.core
import org.apache.s2graph.core.{Management, GraphUtil}
import org.apache.s2graph.core.tinkerpop.process.S2GraphStepStrategy
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies
import org.apache.tinkerpop.gremlin.structure.Graph.{Exceptions, Features, Variables}
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
import org.apache.tinkerpop.gremlin.structure.{Edge, Graph, Transaction, Vertex}
import play.api.libs.json.{Json, JsValue, JsNumber, JsString}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

object S2Graph {

  TraversalStrategies.GlobalCache.registerStrategies(classOf[S2Graph],
    TraversalStrategies.GlobalCache.getStrategies(classOf[Graph]).clone().addStrategies(S2GraphStepStrategy.instance()))


  val EMPTY_CONFIGURATION = new BaseConfiguration() {
    setProperty(Graph.GRAPH, classOf[S2Graph].getClass.getName())
  }
//  private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
//    this.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
//  }};
  def open(configuration: Configuration): S2Graph = {
    new S2Graph(configuration)
  }
}
class S2Graph(val configuration: Configuration) extends Graph(configuration) {

  import scala.collection.JavaConversions._
  val _features = new S2GraphFeatures()
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  val ec = ExecutionContext.fromExecutor(threadPool)

  val WriteRPCTimeOut = 1000
  def convertConfig: Config = {
    val config = ConfigFactory.load()
    val props = for {
      key <- configuration.getKeys
    } yield {
      key -> configuration.getProperty(key)
    }
    config.withFallback(ConfigFactory.parseMap(props.toMap))
  }

  val client = new core.Graph(convertConfig)(ec)

  override def vertices(objects: AnyRef*): util.Iterator[Vertex] = ???

  override def tx(): Transaction = throw Exceptions.transactionsNotSupported()

  override def edges(objects: AnyRef*): util.Iterator[Edge] = ???

  override def variables(): Variables = ???

  /** TODO: consider reasonable fallback */
  override def addVertex(objects: AnyRef*): Vertex = {
    var ts = System.currentTimeMillis()
    var op = "insert"
    var idOpt: Option[String] = None
    var serviceNameOpt: Option[String] = None
    var columnNameOpt: Option[String] = None

    val props = for {
      kv <- objects.grouped(2)
      (k, v) = (kv.head, kv.last)
    } yield {
        k match {
          case "timestamp" => ts = v.toString.toLong
          case "op" => op = v.toString
          case "id" => idOpt = Option(v.toString)
          case "serviceName" => serviceNameOpt = Option(v.toString)
          case "columnName" => columnNameOpt = Option(v.toString)
          case _ =>
        }
        val jsValue: JsValue = v match {
          case s: String => JsString(s)
          case n: Int | Long | Float | Double => JsNumber(n)
          case _ => throw new RuntimeException(s"not supported value type. $v")
        }
        k.toString -> jsValue
      }


    val vertexOpt = for {
      id <- idOpt
      serviceName <- serviceNameOpt
      columnName <- columnNameOpt
    } yield {
      val propsStr = Json.toJson(props.toMap).toString
      Management.toVertex(ts, op, id, serviceName, columnName, propsStr)
    }
    val vertex = vertexOpt.getOrElse(throw new RuntimeException("not all necessary data is provided."))
    val future = client.mutateVertices(Seq(vertex), withWait = true).map { rets =>
      if (rets.forall(identity)) toS2Vertex(vertex)
      else throw new RuntimeException("mutate vertex into storage failed.")
    }
    Await.result(future, Duration(WriteRPCTimeOut, TimeUnit.MILLISECONDS))
  }

  override def close(): Unit = client.shutdown()

  override def compute[C <: GraphComputer](aClass: Class[C]): C = ???

  override def compute(): GraphComputer = ???

  val DefaultVertexLabel = ""

  def toS2Vertex(vertex: core.Vertex): S2Vertex = {
    val vId = vertex.innerId.toIdString()
    val serviceName = vertex.service.serviceName
    val columnName = vertex.serviceColumn.columnName
    new S2Vertex(this, vId, serviceName, columnName, DefaultVertexLabel)
  }


  class S2GraphGraphFeature extends Features.GraphFeatures {

    override def supportsComputer(): Boolean = false

    override def supportsThreadedTransactions(): Boolean = false

    override def supportsTransactions(): Boolean = false

    override def supportsPersistence(): Boolean = true

    override def variables(): Features.VariableFeatures = new Features.VariableFeatures() {}

    override def supportsConcurrentAccess(): Boolean = true

  }

  class S2GraphVertexFeature extends Features.VertexFeatures {

    override def supportsAddVertices(): Boolean = true

    override def supportsRemoveVertices(): Boolean = true

    override def getCardinality(key: String): Cardinality = Cardinality.single

    override def supportsMultiProperties(): Boolean = false

    override def supportsMetaProperties(): Boolean = false

    override def properties(): Features.VertexPropertyFeatures = super.properties()

  }

  class S2GraphEdgeFeature extends Features.EdgeFeatures {

    override def supportsRemoveEdges(): Boolean = true

    override def supportsAddEdges(): Boolean = true

    override def properties(): Features.EdgePropertyFeatures = super.properties()

  }

  class S2GraphFeatures extends Features {
    val graphFeatures = new S2GraphGraphFeature()
    val edgeFeatures = new S2GraphEdgeFeature()
    val vertexFeatures = new S2GraphVertexFeature()
  }

  override def features(): Features = _features
}
