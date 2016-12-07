package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features

class S2EdgeFeatures extends S2ElementFeatures with Features.EdgeFeatures {
  override def supportsRemoveEdges(): Boolean = false

  override def supportsAddEdges(): Boolean = false

  override def properties(): Features.EdgePropertyFeatures = new S2EdgePropertyFeatures
}
