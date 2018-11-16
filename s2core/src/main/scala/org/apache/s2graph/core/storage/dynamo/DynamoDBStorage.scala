package org.apache.s2graph.core.storage.dynamo

import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage._
import com.amazonaws.services.dynamodbv2._

object DynamoDBStorage {

}
class DynamoDBStorage(override val graph: S2GraphLike,
                      override val config: Config) extends Storage(graph, config) {
  private lazy val optimisticEdgeFetcher = new DynamoDBOptimisticEdgeFetcher
  private lazy val optimisitcMutator = new DynamoDBOptimisticMutator

  override val management: StorageManagement = new DynamoDBStorageManagement
  override val serDe: StorageSerDe = new DynamoDBStorageSerDe

  override val edgeFetcher: EdgeFetcher = new DynamoDBEdgeFetcher
  override val vertexFetcher: VertexFetcher = new DynamoDBVertexFetcher
  override val edgeMutator: EdgeMutator = new DefaultOptimisticEdgeMutator(graph, serDe, optimisticEdgeFetcher, optimisitcMutator, io)
  override val vertexMutator: VertexMutator = new DefaultOptimisticVertexMutator(graph, serDe, optimisticEdgeFetcher, optimisitcMutator, io)
}
