package org.apache.s2graph.core.storage.dynamo

import org.apache.s2graph.core.{QueryRequest, S2EdgeLike}
import org.apache.s2graph.core.storage.{OptimisticEdgeFetcher, SKeyValue, StorageIO}

import scala.concurrent.{ExecutionContext, Future}

class DynamoDBOptimisticEdgeFetcher extends OptimisticEdgeFetcher {
  /**
    * translate storage specific data hold class into IndexEdge/SnapshotEdge/Vertex.
    */
  override val io: StorageIO = _

  /**
    *
    * @param queryRequest
    * @param edge
    * @param ec
    */
  override def fetchKeyValues(queryRequest: QueryRequest, edge: S2EdgeLike)(implicit ec: ExecutionContext): Future[Seq[SKeyValue]] = ???
}
