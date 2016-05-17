package org.apache.s2graph.core.tinkerpop.structure


import org.apache.s2graph.core
import org.apache.s2graph.core.GraphUtil._
import org.apache.s2graph.core.mysqls.{Service, ServiceColumn}
import org.apache.s2graph.core.tinkerpop.process.S2GraphStepStrategy
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{GraphUtil, Management}
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphStepStrategy
import play.api.libs.json.{JsValue, Json, JsString}

//import org.apache.s2graph.core.tinkerpop.process.S2GraphStepStrategy
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
//import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies
import org.apache.tinkerpop.gremlin.structure.Graph.{Exceptions, Features, Variables}
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure.{Edge, Graph, Transaction, Vertex}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.collection.JavaConversions._
import java.util
import java.util.concurrent.{Executors, TimeUnit}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.{BaseConfiguration, Configuration}


object S2Graph {

  // clone() does not work because of permission from java and scala.
  val strategies = (new DefaultTraversalStrategies()).addStrategies(S2GraphStepStrategy.instance())

//  val defaultStrategies = TraversalStrategies.GlobalCache.getStrategies(classOf[Graph])
//  val strategies = defaultStrategies.addStrategies(S2GraphStepStrategy.instance())
  TraversalStrategies.GlobalCache.registerStrategies(classOf[S2Graph], strategies)

  val DefaultVertexLabel = ""

  def open(configuration: Configuration): S2Graph = {
    new S2Graph(configuration)
  }

  def toConfig(configuration: Configuration): Config = {
    val config = ConfigFactory.load()
    val javaProps = new util.HashMap[String, AnyRef]()

    val props = for {
      key <- configuration.getKeys
    } yield {
        javaProps.put(key, configuration.getProperty(key))
      }
//    val javaProps: java.util.Map[String, Any] = props.toMap
    config.withFallback(ConfigFactory.parseMap(javaProps))
  }

  def fromConfig(config: Config): Configuration = {
    val configuration = new BaseConfiguration()
    for {
      entry <- config.entrySet()
    } {
      configuration.setProperty(entry.getKey, entry.getValue.unwrapped())
    }
    configuration
  }
  
  def toS2Graph(graph: core.Graph): S2Graph =
    new S2Graph(fromConfig(graph.config))

  def toS2Vertex(s2Graph: S2Graph, vertex: core.Vertex): S2Vertex = {
    new S2Vertex(s2Graph, vertex, DefaultVertexLabel)
  }

  def toS2Edge(s2Graph: S2Graph, edge: core.Edge, label: String): S2Edge = {
    new S2Edge(s2Graph, edge, label)
  }

  def toVertexLabel(serviceName: String, columnName: String): String =
    Seq(serviceName, columnName).mkString(",")

  def fromVertexLabel(vertexLabel: String): (String, String) = {
    val t = vertexLabel.split(",")
    assert(t.length == 2)
    (t.head, t.last)
  }

  def toVertexId(props: Map[String, JsValue]): Option[String] = {
    (props.get("key"), props.get("id")) match {
      case (Some(k), _) => Option(jsValueToStr(k))
      case (_, Some(id)) => Option(jsValueToStr(id))
      case _ => None
    }
  }
}
class S2Graph(val configuration: Configuration) extends Graph {

  import S2Graph._
  val _features = new S2GraphFeatures()
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  implicit val ec = ExecutionContext.fromExecutor(threadPool)

  val WriteRPCTimeOut = 1000



  val client = new core.Graph(S2Graph.toConfig(configuration))(ec)

  /** this looks problematic when underlying graph is large */
  override def vertices(objects: AnyRef*): util.Iterator[Vertex] = Nil.iterator

  /** this looks problematic when underlying graph is large */
  override def edges(edgeIds: AnyRef*): util.Iterator[Edge] = Nil.iterator

  override def tx(): Transaction = throw Exceptions.transactionsNotSupported()

  override def variables(): Variables = ???

  def toS2Vertex(vertex: core.Vertex): S2Vertex =
    new S2Vertex(this, vertex, DefaultVertexLabel)

  def toS2Edge(edge: core.Edge, label: String): S2Edge =
    new S2Edge(this, edge, label)

  /** TODO: consider reasonable fallback */

  override def addVertex(objects: AnyRef*): Vertex = {
    val props = Management.toPropsJson(objects)
    logger.debug(s"[S2Graph#ParsedProps]: $props")

    def throwEx(key: String) = throw new RuntimeException(s"$key is not provided. $props")

    import GraphUtil._
    val id = toVertexId(props).getOrElse(throwEx("key / id"))
    val serviceName = props.get("serviceName").map(jsValueToStr(_)).getOrElse(throwEx("serviceName"))
    val columnName = props.get("columnName").map(jsValueToStr(_)).getOrElse(throwEx("columnName"))
    val service = Service.findByName(serviceName).getOrElse(throwEx("service"))
    val serviceColumn = ServiceColumn.find(service.id.get, columnName).getOrElse(throwEx("serviceColumn"))

    val vertex = Management.toVertexWithServiceColumn(serviceColumn)(id)(props.toSeq: _*)


    val future = client.mutateVertices(Seq(vertex), withWait = true).map { rets =>
      if (rets.forall(identity)) toS2Vertex(vertex)
      else throw new RuntimeException("mutate vertex into storage failed.")
    }
    Await.result(future, Duration(WriteRPCTimeOut, TimeUnit.MILLISECONDS))
  }

  override def close(): Unit = client.shutdown()

  override def compute[C <: GraphComputer](aClass: Class[C]): C = ???

  override def compute(): GraphComputer = ???






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
