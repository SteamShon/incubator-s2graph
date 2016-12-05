package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features

class S2VertexPropertyFeatures extends S2PropertyFeatures with Features.VertexPropertyFeatures {
  override def supportsStringIds(): Boolean = false

  override def supportsCustomIds(): Boolean = false

  override def supportsUuidIds(): Boolean = false

  override def supportsAddProperty(): Boolean = false

  override def supportsUserSuppliedIds(): Boolean = false

  override def supportsRemoveProperty(): Boolean = false

  override def supportsAnyIds(): Boolean = false

  override def supportsNumericIds(): Boolean = false

  override def willAllowId(id: scala.Any): Boolean = false
}
