package org.apache.s2graph.core.Integrate.tinkerpop.structure

import org.apache.s2graph.core.Integrate.tinkerpop.S2GraphProvider
import org.apache.s2graph.core.S2Graph
import org.apache.tinkerpop.gremlin.GraphProviderClass
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite
import org.junit.runner.RunWith

@RunWith(classOf[StructureStandardSuite])
@GraphProviderClass(provider = classOf[S2GraphProvider], graph = classOf[S2Graph])
class S2GraphStructureStandardTest {

}