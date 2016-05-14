package org.apache.s2graph.core.tinkerpop.structure


import org.apache.s2graph.core
import org.apache.s2graph.core.mysqls.{Service, ServiceColumn}
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{GraphUtil, Management}
import play.api.libs.json.{Json, JsString}

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

//  TraversalStrategies.GlobalCache.registerStrategies(classOf[S2Graph],
//    TraversalStrategies.GlobalCache.getStrategies(classOf[Graph]).clone().addStrategies(S2GraphStepStrategy.instance()))
//
//
//  val EMPTY_CONFIGURATION = new BaseConfiguration() {
//    setProperty(Graph.GRAPH, classOf[S2Graph].getClass.getName())
//  }
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
    import GraphUtil._

    val vertexOpt = for {
      id <- props.get("id").map(jsValueToStr(_))
      serviceName <- props.get("serviceName").map(jsValueToStr(_))
      columnName <- props.get("columnName").map(jsValueToStr(_))
      service <- Service.findByName(serviceName)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName)
    } yield Management.toVertexWithServiceColumn(serviceColumn)(props.toSeq: _*)


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
