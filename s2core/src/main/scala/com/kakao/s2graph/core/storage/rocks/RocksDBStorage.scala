package com.kakao.s2graph.core.storage.rocks

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Base64
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.Serializable._
import com.kakao.s2graph.core.storage._
import com.kakao.s2graph.core.storage.rocks.RocksDBHelper._
import com.kakao.s2graph.core.storage.serde.indexedge
import com.kakao.s2graph.core.storage.serde.snapshotedge.tall
import com.kakao.s2graph.core.types.HBaseType._
import com.kakao.s2graph.core.types.VertexId
import com.kakao.s2graph.core.utils.logger
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.rocksdb._

import scala.collection.Seq
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

object RocksDBHelper {
  def longToBytes(value: Long): Array[Byte] = {
    val longBuffer = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder())
    longBuffer.clear()
    longBuffer.putLong(value)
    longBuffer.array()
  }

  def bytesToLong(data: Array[Byte], offset: Int): Long = {
    if (data != null) {
      val longBuffer = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder())
      longBuffer.put(data, offset, 8)
      longBuffer.flip()
      longBuffer.getLong()
    } else 0L
  }

  trait Request
  case class ScanRequest(startKey: Array[Byte], stopKey: Array[Byte], limit: Int) extends Request
  case class GetRequest(startKey: Array[Byte]) extends Request
  case class WriteLockRequest(kv: SKeyValue, expectedOpt: Option[SKeyValue]) extends Request
  case class WriteRequest(cluster: String, kvs: Seq[SKeyValue]) extends Request
}
/**
 * currently RocksDBStorage only support schema_v4.
 * @param config
 * @param ec
 */
class RocksDBStorage(override val config: Config)(implicit ec: ExecutionContext)
  extends Storage[QueryRequestWithResult](config) {

  implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  val system = ActorSystem("RocksDBStorage", config)
  val poolSize = 2
  val actors: Seq[ActorRef] = (0 until poolSize).map { ith =>
    system.actorOf(Props(new RocksDBActor(ith)))
  }
  /** we use tall schema for both snapshot edge and indexedge in RocksDBStorage. */
  override def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge) =
    new serde.snapshotedge.tall.SnapshotEdgeSerializable(snapshotEdge)

  override def indexEdgeSerializer(indexEdge: IndexEdge) =
    new serde.indexedge.tall.IndexEdgeSerializable(indexEdge)

  override def vertexSerializer(vertex: Vertex) =
    new serde.vertex.tall.VertexSerializable(vertex)

  override val snapshotEdgeDeserializers = Map(
    VERSION1 -> new tall.SnapshotEdgeDeserializable,
    VERSION2 -> new tall.SnapshotEdgeDeserializable,
    VERSION3 -> new tall.SnapshotEdgeDeserializable,
    VERSION4 -> new tall.SnapshotEdgeDeserializable
  )

  override val indexEdgeDeserializers = Map(
    VERSION1 -> new indexedge.tall.IndexEdgeDeserializable(RocksDBHelper.bytesToLong),
    VERSION2 -> new indexedge.tall.IndexEdgeDeserializable(RocksDBHelper.bytesToLong),
    VERSION3 -> new indexedge.tall.IndexEdgeDeserializable(RocksDBHelper.bytesToLong),
    VERSION4 -> new indexedge.tall.IndexEdgeDeserializable(RocksDBHelper.bytesToLong)
  )

  override val vertexDeserializers = Map(
    VERSION1 -> new serde.vertex.tall.VertexDeserializable,
    VERSION2 -> new serde.vertex.tall.VertexDeserializable,
    VERSION3 -> new serde.vertex.tall.VertexDeserializable,
    VERSION4 -> new serde.vertex.tall.VertexDeserializable
  )


  def withShard[T: ClassTag](key: Array[Byte], request: Request): Future[T] = {
    val k = Math.abs(MurmurHash3.arrayHash(key))
    val shard = actors(k % poolSize)
    (shard ? request).mapTo[T]
  }

  override def writeToStorage(cluster: String, kvs: Seq[SKeyValue], withWait: Boolean): Future[Boolean] = {
    if (kvs.isEmpty) {
      Future.successful(true)
    } else {
      val kv = kvs.head
      withShard[Boolean](kv.row.take(2), WriteRequest(cluster, kvs))
    }
  }


  override def createTable(zkAddr: String,
                           tableName: String,
                           cfs: List[String],
                           regionMultiplier: Int,
                           ttl: Option[Int],
                           compressionAlgorithm: String): Unit = {
    // nothing to do for now.
  }

  override def fetchSnapshotEdgeKeyValues(queryParamWithStartStopKeyRange: AnyRef): Future[Seq[SKeyValue]] = {
    queryParamWithStartStopKeyRange match {
      case (queryParam: QueryParam, (startKey: Array[Byte], stopKey: Array[Byte])) =>
        withShard[Seq[SKeyValue]](startKey.take(2), GetRequest(startKey))
      case _ => Future.successful(Seq.empty)
    }
  }

  override def buildRequest(queryRequest: QueryRequest): (QueryParam, (Array[Byte], Array[Byte])) = {
    queryRequest.queryParam.tgtVertexInnerIdOpt match {
      case None => // indexEdges
        val queryParam = queryRequest.queryParam
        val edge = toRequestEdge(queryRequest)
        val indexEdgeOpt = edge.edgesWithIndex.filter(edgeWithIndex => edgeWithIndex.labelIndex.seq == queryParam.labelOrderSeq).headOption
        val indexEdge = indexEdgeOpt.getOrElse(throw new RuntimeException(s"Can`t find index for query $queryParam"))
        val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
        val labelWithDirBytes = indexEdge.labelWithDir.bytes
        val labelIndexSeqWithIsInvertedBytes = StorageSerializable.labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)

        val baseKey = Bytes.add(srcIdBytes, labelWithDirBytes, Bytes.add(labelIndexSeqWithIsInvertedBytes, Array.fill(1)(edge.op)))
        val (startKey, stopKey) =
          if (queryParam.columnRangeFilter != null) {
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Bytes.add(Base64.getDecoder.decode(cursor), Array.fill(1)(0))
              case None => Bytes.add(baseKey, queryParam.columnRangeFilterMinBytes)
            }
            (_startKey, Bytes.add(baseKey, queryParam.columnRangeFilterMaxBytes))
          } else {
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Bytes.add(Base64.getDecoder.decode(cursor), Array.fill(1)(0))
              case None => baseKey
            }
            (_startKey, Bytes.add(baseKey, Array.fill(1)(-1)))
          }
        (queryRequest.queryParam, (startKey, stopKey))
      case Some(tgtId) => // snapshotEdge
        val kv = snapshotEdgeSerializer(toRequestEdge(queryRequest).toSnapshotEdge).toKeyValues.head
        (queryRequest.queryParam, (kv.row, kv.row))
    }
  }
  override def fetch(queryRequest: QueryRequest,
                     prevStepScore: Double,
                     isInnerCall: Boolean,
                     parentEdges: Seq[EdgeWithScore]): QueryRequestWithResult = throw new RuntimeException("not necessary")


  override def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]] = {
    val futures = for {
      (queryRequest, prevStepScore) <- queryRequestWithScoreLs
      parentEdges <- prevStepEdges.get(queryRequest.vertex.id)
    } yield {
        val request = buildRequest(queryRequest)
        val (queryParam, (startKey, stopKey)) = request
        withShard[Seq[SKeyValue]](startKey.take(2), ScanRequest(startKey, stopKey, queryParam.limit)).map { kvs =>
          val edgeWithScores = toEdges(kvs, queryParam, prevStepScore, false, parentEdges).filter { case edgeWithScore =>
            val edge = edgeWithScore.edge
            val duration = queryParam.duration.getOrElse((Long.MinValue, Long.MaxValue))
            edge.ts >= duration._1 && edge.ts < duration._2
          }
          val resultEdgesWithScores =
            if (queryRequest.queryParam.sample >= 0) sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
            else edgeWithScores

          QueryRequestWithResult(queryRequest, QueryResult(resultEdgesWithScores, tailCursor = kvs.lastOption.map(_.row).getOrElse(Array.empty)))
        }
      }
    Future.sequence(futures)
    //    Future.successful(futures)
  }

  override def writeLock(rpc: SKeyValue, expectedOpt: Option[SKeyValue]): Future[Boolean] = {
    withShard[Boolean](rpc.row.take(2), WriteLockRequest(rpc, expectedOpt))
  }

  /** Management Logic */
  override def flush(): Unit = {
//    db.flush(new FlushOptions().setWaitForFlush(true))
//    db.close()
  }

  override def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]] = {
    Future.successful(Seq.empty)
  }

  /**
   * fetch Vertex for given request from storage.
   * @param request
   * @return
   */
  override def fetchVertexKeyValues(request: AnyRef): Future[scala.Seq[SKeyValue]] = fetchSnapshotEdgeKeyValues(request)
}

class RocksDBActor(idx: Int) extends Actor {

  private val emptyBytes = Array.empty[Byte]
  private val table = Array.empty[Byte]
  private val qualifier = Array.empty[Byte]

  RocksDB.loadLibrary()

  //  val env = new RocksEnv()

  private val memEnv = new RocksMemEnv()
  private val writeOptions = new WriteOptions()
  private val readOptions = new ReadOptions().setFillCache(true).setVerifyChecksums(false)

  private val options = new Options()
    .setCreateIfMissing(true)
    .setWriteBufferSize(1024 * 1024 * 512)
    .setMergeOperatorName("uint64add")
    .createStatistics()
    .setDbLogDir("./")
    .setTableCacheNumshardbits(5)
    .setIncreaseParallelism(8)
    .setAllowOsBuffer(true)
    .setAllowMmapReads(true)
    .setArenaBlockSize(1024 * 32)


  //    .setOptimizeFiltersForHits(true)


  private var db: RocksDB = null


  try {
    // a factory method that returns a RocksDB instance
    // only for testing now.

    db = RocksDB.open(options, s"/tmp/rocks_$idx")
  } catch {
    case e: RocksDBException =>
      logger.error(s"initialize rocks db storage failed.", e)
  }

  private val cacheLoader = new CacheLoader[String, ReentrantLock] {
    override def load(key: String) = new ReentrantLock()
  }
  val LockExpireDuration = 10000

  private val lockMap = CacheBuilder.newBuilder()
    .concurrencyLevel(124)
    .expireAfterAccess(LockExpireDuration, TimeUnit.MILLISECONDS)
    .expireAfterWrite(LockExpireDuration, TimeUnit.MILLISECONDS)
    .maximumSize(1000 * 10 * 10 * 10 * 10)
    .build[String, ReentrantLock](cacheLoader)

  private def withLock[A](key: Array[Byte])(op: => A): A = {
    val lockKey = Bytes.toString(key)
    val lock = lockMap.get(lockKey)

    try {
      lock.lock
      op
    } finally {
      lock.unlock()
    }
  }

  private def buildWriteBatch(kvs: Seq[SKeyValue]): WriteBatch = {
    val writeBatch = new WriteBatch()
    kvs.foreach { kv =>
      kv.operation match {
        case SKeyValue.Put => writeBatch.put(kv.row, kv.value)
        case SKeyValue.Delete => writeBatch.remove(kv.row)
        case SKeyValue.Increment =>
          val javaLong = Bytes.toLong(kv.value)
          writeBatch.merge(kv.row, RocksDBHelper.longToBytes(javaLong))
        case _ => throw new RuntimeException(s"not supported rpc operation. ${kv.operation}")
      }
    }
    writeBatch
  }

  private def getKeyValue(key: Array[Byte]): Seq[SKeyValue] = {
    val v = db.get(readOptions, key)
    if (v == null) Seq.empty
    else Seq(SKeyValue(table, key, edgeCf, qualifier, v, System.currentTimeMillis()))
  }

  private def scanKeyValues(startKey: Array[Byte], endKey: Array[Byte], limit: Int): Seq[SKeyValue] = {
    val iter = db.newIterator(readOptions)
    try {
      var idx = 0
      iter.seek(startKey)

      val kvs = new ListBuffer[SKeyValue]()
      val ts = System.currentTimeMillis()
      iter.seek(startKey)
      while (iter.isValid && Bytes.compareTo(iter.key, endKey) <= 0 && idx < limit) {
        kvs += SKeyValue(table, iter.key, edgeCf, qualifier, iter.value, System.currentTimeMillis())
        iter.next()
        idx += 1
      }
      kvs.toSeq
    } finally {
      iter.dispose()
    }
  }

  private def writeLock(kv: SKeyValue, expectedOpt: Option[SKeyValue]): Boolean = {
    def op: Boolean = {
      try {
        val fetchedValue = db.get(kv.row)
        val innerRet = expectedOpt match {
          case None =>
            if (fetchedValue == null) {
              db.put(writeOptions, kv.row, kv.value)
              true
            } else {
              false
            }
          case Some(kv) =>
            if (fetchedValue == null) {
              false
            } else {
              if (Bytes.compareTo(fetchedValue, kv.value) == 0) {
                db.put(writeOptions, kv.row, kv.value)
                true
              } else {
                false
              }
            }
        }
        innerRet
      } catch {
        case e: RocksDBException =>
          logger.error(s"Write lock failed", e)
          false
      }
    }
    withLock(kv.row)(op)
  }


  override def receive: Actor.Receive = {
    case request: WriteRequest =>
      val (cluster, kvs) = WriteRequest.unapply(request).get
      val writeBatch = buildWriteBatch(kvs)
      val ret = try {
        db.write(writeOptions, writeBatch)
        true
      } catch {
        case e: Exception =>
          logger.error(s"writeAsyncSimple failed.", e)
          false
      } finally {
        writeBatch.dispose()
      }
      sender ! ret

    case request: GetRequest =>
      sender ! getKeyValue(request.startKey)

    case request: ScanRequest =>
      val (startKey, stopKey, limit) = ScanRequest.unapply(request).get
      sender ! scanKeyValues(startKey, stopKey, limit)

    case request: WriteLockRequest =>
      val (kv, expectedOpt) = WriteLockRequest.unapply(request).get
      sender ! writeLock(kv, expectedOpt)
  }
}
