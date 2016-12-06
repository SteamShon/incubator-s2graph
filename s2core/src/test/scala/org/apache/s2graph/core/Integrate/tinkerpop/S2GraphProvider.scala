package org.apache.s2graph.core.Integrate.tinkerpop

import java.util

import org.apache.commons.configuration.Configuration
import org.apache.s2graph.core._
import org.apache.tinkerpop.gremlin.AbstractGraphProvider
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData
import org.apache.tinkerpop.gremlin.structure.Graph
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object S2GraphProvider {
  val Implementation: Set[Class[_]] = Set(
    classOf[S2Edge],
    classOf[S2Vertex],
    classOf[S2Property[_]],
    classOf[S2VertexProperty[_]],
    classOf[S2Graph]
  )
}
class S2GraphProvider extends AbstractGraphProvider {

  override def getBaseConfiguration(s: String, aClass: Class[_], s1: String, graphData: GraphData): util.Map[String, AnyRef] = {
    S2Graph.DefaultConfig.entrySet().map(e => e.getKey -> e.getValue.unwrapped()).toMap.asJava
  }

  override def clear(graph: Graph, configuration: Configuration): Unit = {

  }

  override def getImplementations: util.Set[Class[_]] = S2GraphProvider.Implementation.asJava
}
//public class TinkerGraphProvider extends AbstractGraphProvider {
//
//  private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
//        add(TinkerEdge.class);
//        add(TinkerElement.class);
//        add(TinkerGraph.class);
//        add(TinkerGraphVariables.class);
//        add(TinkerProperty.class);
//        add(TinkerVertex.class);
//        add(TinkerVertexProperty.class);
//    }};
//
//  @Override
//  public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName,
//  final LoadGraphWith.GraphData loadGraphWith) {
//    final TinkerGraph.DefaultIdManager idManager = selectIdMakerFromGraphData(loadGraphWith);
//    final String idMaker = (idManager.equals(TinkerGraph.DefaultIdManager.ANY) ? selectIdMakerFromTest(test, testMethodName) : idManager).name();
//    return new HashMap<String, Object>() {{
//            put(Graph.GRAPH, TinkerGraph.class.getName());
//            put(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, idMaker);
//            put(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, idMaker);
//            put(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, idMaker);
//            if (requiresListCardinalityAsDefault(loadGraphWith, test, testMethodName))
//                put(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name());
//            if (requiresPersistence(test, testMethodName)) {
//                put(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
//                final File tempDir = TestHelper.makeTestDataPath(test, "temp");
//                put(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION,
//                        tempDir.getAbsolutePath() + File.separator + testMethodName + ".kryo");
//            }
//        }};
//  }
//
//  @Override
//  public void clear(final Graph graph, final Configuration configuration) throws Exception {
//    if (graph != null)
//      graph.close();
//
//    // in the even the graph is persisted we need to clean up
//    final String graphLocation = configuration.getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, null);
//    if (graphLocation != null) {
//      final File f = new File(graphLocation);
//      f.delete();
//    }
//  }
//
//  @Override
//  public Set<Class> getImplementations() {
//        return IMPLEMENTATION;
//    }
//
//  /**
//   * Determines if a test requires TinkerGraph persistence to be configured with graph location and format.
//   */
//  protected static boolean requiresPersistence(final Class<?> test, final String testMethodName) {
//    return test == GraphTest.class && testMethodName.equals("shouldPersistDataOnClose");
//  }
//
//  /**
//   * Determines if a test requires a different cardinality as the default or not.
//   */
//  protected static boolean requiresListCardinalityAsDefault(final LoadGraphWith.GraphData loadGraphWith,
//  final Class<?> test, final String testMethodName) {
//    return loadGraphWith == LoadGraphWith.GraphData.CREW
//    || (test == StarGraphTest.class && testMethodName.equals("shouldAttachWithCreateMethod"))
//    || (test == DetachedGraphTest.class && testMethodName.equals("testAttachableCreateMethod"));
//  }
//
//  /**
//   * Some tests require special configuration for TinkerGraph to properly configure the id manager.
//   */
//  protected TinkerGraph.DefaultIdManager selectIdMakerFromTest(final Class<?> test, final String testMethodName) {
//    if (test.equals(GraphTest.class)) {
//      final Set<String> testsThatNeedLongIdManager = new HashSet<String>(){{
//                add("shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentation");
//                add("shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentations");
//                add("shouldIterateVerticesWithNumericIdSupportUsingIntegerRepresentation");
//                add("shouldIterateVerticesWithNumericIdSupportUsingIntegerRepresentations");
//                add("shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentation");
//                add("shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentations");
//                add("shouldIterateVerticesWithNumericIdSupportUsingStringRepresentation");
//                add("shouldIterateVerticesWithNumericIdSupportUsingStringRepresentations");
//                add("shouldIterateEdgesWithNumericIdSupportUsingDoubleRepresentation");
//                add("shouldIterateEdgesWithNumericIdSupportUsingDoubleRepresentations");
//                add("shouldIterateEdgesWithNumericIdSupportUsingIntegerRepresentation");
//                add("shouldIterateEdgesWithNumericIdSupportUsingIntegerRepresentations");
//                add("shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentation");
//                add("shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentations");
//                add("shouldIterateEdgesWithNumericIdSupportUsingStringRepresentation");
//                add("shouldIterateEdgesWithNumericIdSupportUsingStringRepresentations");
//            }};
//
//      final Set<String> testsThatNeedUuidIdManager = new HashSet<String>(){{
//                add("shouldIterateVerticesWithUuidIdSupportUsingStringRepresentation");
//                add("shouldIterateVerticesWithUuidIdSupportUsingStringRepresentations");
//                add("shouldIterateEdgesWithUuidIdSupportUsingStringRepresentation");
//                add("shouldIterateEdgesWithUuidIdSupportUsingStringRepresentations");
//            }};
//
//      if (testsThatNeedLongIdManager.contains(testMethodName))
//        return TinkerGraph.DefaultIdManager.LONG;
//      else if (testsThatNeedUuidIdManager.contains(testMethodName))
//        return TinkerGraph.DefaultIdManager.UUID;
//    }  else if (test.equals(IoEdgeTest.class)) {
//      final Set<String> testsThatNeedLongIdManager = new HashSet<String>(){{
//                add("shouldReadWriteEdge[graphson-v1]");
//                add("shouldReadWriteDetachedEdgeAsReference[graphson-v1]");
//                add("shouldReadWriteDetachedEdge[graphson-v1]");
//                add("shouldReadWriteEdge[graphson-v2]");
//                add("shouldReadWriteDetachedEdgeAsReference[graphson-v2]");
//                add("shouldReadWriteDetachedEdge[graphson-v2]");
//            }};
//
//      if (testsThatNeedLongIdManager.contains(testMethodName))
//        return TinkerGraph.DefaultIdManager.LONG;
//    } else if (test.equals(IoVertexTest.class)) {
//      final Set<String> testsThatNeedLongIdManager = new HashSet<String>(){{
//                add("shouldReadWriteVertexWithBOTHEdges[graphson-v1]");
//                add("shouldReadWriteVertexWithINEdges[graphson-v1]");
//                add("shouldReadWriteVertexWithOUTEdges[graphson-v1]");
//                add("shouldReadWriteVertexNoEdges[graphson-v1]");
//                add("shouldReadWriteDetachedVertexNoEdges[graphson-v1]");
//                add("shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v1]");
//                add("shouldReadWriteVertexMultiPropsNoEdges[graphson-v1]");
//                add("shouldReadWriteVertexWithBOTHEdges[graphson-v2]");
//                add("shouldReadWriteVertexWithINEdges[graphson-v2]");
//                add("shouldReadWriteVertexWithOUTEdges[graphson-v2]");
//                add("shouldReadWriteVertexNoEdges[graphson-v2]");
//                add("shouldReadWriteDetachedVertexNoEdges[graphson-v2]");
//                add("shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v2]");
//                add("shouldReadWriteVertexMultiPropsNoEdges[graphson-v2]");
//            }};
//
//      if (testsThatNeedLongIdManager.contains(testMethodName))
//        return TinkerGraph.DefaultIdManager.LONG;
//    }
//
//    return TinkerGraph.DefaultIdManager.ANY;
//  }
//
//  /**
//   * Test that load with specific graph data can be configured with a specific id manager as the data type to
//   * be used in the test for that graph is known.
//   */
//  protected TinkerGraph.DefaultIdManager selectIdMakerFromGraphData(final LoadGraphWith.GraphData loadGraphWith) {
//    if (null == loadGraphWith) return TinkerGraph.DefaultIdManager.ANY;
//    if (loadGraphWith.equals(LoadGraphWith.GraphData.CLASSIC))
//      return TinkerGraph.DefaultIdManager.INTEGER;
//    else if (loadGraphWith.equals(LoadGraphWith.GraphData.MODERN))
//      return TinkerGraph.DefaultIdManager.INTEGER;
//    else if (loadGraphWith.equals(LoadGraphWith.GraphData.CREW))
//      return TinkerGraph.DefaultIdManager.INTEGER;
//    else if (loadGraphWith.equals(LoadGraphWith.GraphData.GRATEFUL))
//      return TinkerGraph.DefaultIdManager.INTEGER;
//    else
//      throw new IllegalStateException(String.format("Need to define a new %s for %s", TinkerGraph.IdManager.class.getName(), loadGraphWith.name()));
//  }
//}