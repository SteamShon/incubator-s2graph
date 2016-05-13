package org.apache.s2graph.core.tinkerpop.structure


import org.apache.s2graph.core
import org.apache.s2graph.core.Management
import org.apache.s2graph.core.tinkerpop.process.S2GraphStepStrategy
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies
import org.apache.tinkerpop.gremlin.structure.Graph.{Exceptions, Features, Variables}
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure.{Edge, Graph, Transaction, Vertex}
import play.api.libs.json.{JsNumber, JsString, JsValue, Json}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.collection.JavaConversions._
import java.util
import java.util.concurrent.{Executors, TimeUnit}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.{BaseConfiguration, Configuration}


object S2Graph {

  TraversalStrategies.GlobalCache.registerStrategies(classOf[S2Graph],
    TraversalStrategies.GlobalCache.getStrategies(classOf[Graph]).clone().addStrategies(S2GraphStepStrategy.instance()))


  val EMPTY_CONFIGURATION = new BaseConfiguration() {
    setProperty(Graph.GRAPH, classOf[S2Graph].getClass.getName())
  }

  def open(configuration: Configuration): S2Graph = {
    new S2Graph(configuration)
  }

  def toConfig(configuration: Configuration): Config = {
    val config = ConfigFactory.load()
    val props = for {
      key <- configuration.getKeys
    } yield {
        key -> configuration.getProperty(key)
      }
    config.withFallback(ConfigFactory.parseMap(props.toMap))
  }

  def fromConfig(config: Config): Configuration = {
    val configuration = new BaseConfiguration()
    for {
      (k, v) <- config.entrySet()
    } {
      configuration.setProperty(k, v)
    }
    configuration
  }
  
  def toS2Graph(graph: core.Graph): S2Graph = new S2Graph(fromConfig(graph.config))

}
class S2Graph(val configuration: Configuration) extends Graph(configuration) {


  val _features = new S2GraphFeatures()
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  val ec = ExecutionContext.fromExecutor(threadPool)

  val WriteRPCTimeOut = 1000



  val client = new core.Graph(S2Graph.toConfig(configuration))(ec)

  /** this looks problematic when underlying graph is large */
  override def vertices(objects: AnyRef*): util.Iterator[Vertex] = Nil.iterator

  /** this looks problematic when underlying graph is large */
  override def edges(edgeIds: AnyRef*): util.Iterator[Edge] = Nil.iterator

  override def tx(): Transaction = throw Exceptions.transactionsNotSupported()

  override def variables(): Variables = ???


  def toPropsJson(objects: AnyRef*): Map[String, JsValue] = {
    val props = for {
      kv <- objects.grouped(2)
      (k, v) = (kv.head, kv.last)
    } yield {
        val jsValue: JsValue  = k match {
          case "timestamp" => JsNumber(v.toString.toLong)
          case "op" => JsString(v.toString)
          case "id" => JsString(v.toString)
          case "serviceName" => JsString(v.toString)
          case "columnName" => JsString(v.toString)
          case _ =>
            v match {
              case s: String => JsString(s)
              case n: Int | Long | Float | Double => JsNumber(n)
              case _ => throw new RuntimeException(s"unsupported value type. $v")
            }
        }
        k.toString -> jsValue
      }

    props.toMap
  }

  def toStr(jsValue: JsValue): String = jsValue match {
    case s: JsString => s.value
    case _ => jsValue.toString
  }

  /** TODO: consider reasonable fallback */
  override def addVertex(objects: AnyRef*): Vertex = {
    val props = toPropsJson(objects)

    val vertexOpt = for {
      id <- props.get("id").map(toStr(_))
      serviceName <- props.get("serviceName").map(toStr(_))
      columnName <- props.get("columnName").map(toStr(_))
    } yield {
      val ts = props.get("timestamp").map(v => v.toString().toLong).getOrElse(System.currentTimeMillis())
      val op = toStr(props.getOrElse("operation", JsString("insert")))
      val propsStr = Json.toJson(props).toString
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
    new S2Vertex(this, vertex, DefaultVertexLabel)
  }

  def toS2Edge(edge: core.Edge): S2Edge = {
    new S2Edge(this, edge)
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
