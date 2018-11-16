package org.apache.s2graph.core.storage.dynamo

import org.apache.s2graph.core.storage.serde.Deserializable
import org.apache.s2graph.core.{IndexEdge, S2EdgeLike, S2VertexLike, SnapshotEdge}
import org.apache.s2graph.core.storage.{StorageSerDe, serde}

class DynamoDBStorageSerDe extends StorageSerDe {
  /**
    * create serializer that knows how to convert given snapshotEdge into kvs: Seq[SKeyValue]
    * so we can store this kvs.
    *
    * @param snapshotEdge : snapshotEdge to serialize
    * @return serializer implementation for StorageSerializable which has toKeyValues return Seq[SKeyValue]
    */
  override def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge): serde.Serializable[SnapshotEdge] = ???

  /**
    * create serializer that knows how to convert given indexEdge into kvs: Seq[SKeyValue]
    *
    * @param indexEdge : indexEdge to serialize
    * @return serializer implementation
    */
  override def indexEdgeSerializer(indexEdge: IndexEdge): serde.Serializable[IndexEdge] = ???

  /**
    * create serializer that knows how to convert given vertex into kvs: Seq[SKeyValue]
    *
    * @param vertex : vertex to serialize
    * @return serializer implementation
    */
  override def vertexSerializer(vertex: S2VertexLike): serde.Serializable[S2VertexLike] = ???

  /**
    * create deserializer that can parse stored CanSKeyValue into snapshotEdge.
    * note that each storage implementation should implement implicit type class
    * to convert storage dependent dataType into common SKeyValue type by implementing CanSKeyValue
    *
    * ex) Asynchbase use it's KeyValue class and CanSKeyValue object has implicit type conversion method.
    * if any storaage use different class to represent stored byte array,
    * then that storage implementation is responsible to provide implicit type conversion method on CanSKeyValue.
    **/
  override def snapshotEdgeDeserializer(schemaVer: String): Deserializable[SnapshotEdge] = ???

  override def indexEdgeDeserializer(schemaVer: String): Deserializable[S2EdgeLike] = ???

  override def vertexDeserializer(schemaVer: String): Deserializable[S2VertexLike] = ???
}
