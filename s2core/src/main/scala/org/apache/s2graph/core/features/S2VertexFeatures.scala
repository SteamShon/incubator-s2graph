package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality

class S2VertexFeatures extends S2ElementFeatures with Features.VertexFeatures {
  override def supportsAddVertices(): Boolean = false

  override def supportsRemoveVertices(): Boolean = false

  override def getCardinality(key: String): Cardinality = super.getCardinality(key)

  override def supportsMultiProperties(): Boolean = false

  override def supportsMetaProperties(): Boolean = false

  override def properties(): Features.VertexPropertyFeatures = super.properties()
}
