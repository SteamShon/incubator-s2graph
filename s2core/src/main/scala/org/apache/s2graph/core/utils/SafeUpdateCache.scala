/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.utils

import java.io._
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.cache.CacheBuilder
import com.google.common.hash.Hashing
import io.reactivex.functions.Consumer
import io.reactivex.schedulers.Schedulers

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object SafeUpdateCache {

  case class CacheKey(key: String)

  def serialise(value: AnyRef): Try[Array[Byte]] = {
    import scala.pickling.Defaults._
    import scala.pickling.binary._

    val result = Try(value.pickle.value)
    result.failed.foreach { e =>
      logger.syncInfo(s"[Serialise failed]: ${value}, ${e}")
    }

    result
  }

  def deserialise(bytes: Array[Byte]): Try[AnyRef] = {
    import scala.pickling.Defaults._
    import scala.pickling.binary._

    Try(BinaryPickle(bytes).unpickle[AnyRef])
  }
}

class SafeUpdateCache(maxSize: Int,
                      ttl: Int,
                      synchronizerOpt: Option[Sync[Array[Byte]]] = None)
                     (implicit executionContext: ExecutionContext) {

  import java.lang.{Long => JLong}
  import SafeUpdateCache._

  @volatile var useSync = synchronizerOpt.isDefined

  synchronizerOpt.foreach { sync =>

    /**
      * Backpressure: http://www.baeldung.com/rxjava-backpressure
      *
      * Must specify observerOn to run the subscription in a separate thread.
      *
      * http://tomstechnicalblog.blogspot.kr/2016/02/rxjava-understanding-observeon-and.html
      *
      * [RxComputationScheduler-2] - [Message received] (-6017608668500074083,[B@33b58b4a)
      * [RxComputationScheduler-1] - [Message received] (-6017608668500074083,[B@33b58b4a)
      */
    import io.reactivex._

    sync
      .emitter
      .onBackpressureBuffer(1024, new io.reactivex.functions.Action() {
        override def run() = { logger.error("[Subscribe buffer overflow: drop oldest message]") }
      }, BackpressureOverflowStrategy.DROP_OLDEST)
      .observeOn(Schedulers.computation())
      .subscribe(new functions.Consumer[(String, Array[Byte])] {
        override def accept(t: (String, Array[Byte])) = {
          if (useSync) {
            val (k, value) = t
            val cacheKey = k.toLong

            if (value.isEmpty) {
              invalidateInner(cacheKey)
            } else {
              val deserialized = deserialise(value)
              if (deserialized.isSuccess) {
                putInner(cacheKey, deserialized.get)
              }
            }

            logger.syncInfo(s"[Message received]: key: ${t._1}")
          }
        }
      }, new Consumer[Throwable] { // onError
        override def accept(t: Throwable): Unit = logger.error("[Error on message receive]", t)
      })
  }

  private val cache = CacheBuilder.newBuilder().maximumSize(maxSize)
    .build[JLong, (AnyRef, Int, AtomicBoolean)]()

  private def toCacheKey(key: String): Long = {
    Hashing.murmur3_128().hashBytes(key.getBytes("UTF-8")).asLong()
  }

  private def toTs() = (System.currentTimeMillis() / 1000).toInt

  def put(key: String, value: AnyRef, broadcast: Boolean = false): Unit = {
    val cacheKey = toCacheKey(key)
    cache.put(cacheKey, (value, toTs, new AtomicBoolean(false)))

    if (useSync && broadcast) {
      synchronizerOpt.foreach { obj =>
        serialise(value).foreach { serialised =>
          logger.syncInfo(s"[Broadcast in SafeUpdateCache]: key: ${key}, cacheKey: ${cacheKey}, value: ${value}")
          obj.send(cacheKey.toString, serialised)
        }
      }
    }
  }

  def putInner(cacheKey: Long, value: AnyRef): Unit = {
    cache.put(cacheKey, (value, toTs, new AtomicBoolean(false)))
  }

  def invalidate(key: String, broadcast: Boolean = true) = {
    val cacheKey = toCacheKey(key)
    cache.invalidate(cacheKey)

    if (useSync && broadcast) {
      synchronizerOpt.foreach(obj => obj.send(cacheKey.toString))
    }
  }

  def invalidateInner(cacheKey: Long): Unit = {
    cache.invalidate(cacheKey)
  }

  def withCache[T <: AnyRef](key: String, broadcast: Boolean)(op: => T): T = {
    val cacheKey = toCacheKey(key)
    val cachedValWithTs = cache.getIfPresent(cacheKey)

    if (cachedValWithTs == null) {
      val value = op
      // fetch and update cache.
      put(key, value, broadcast)
      value
    } else {
      val (_cachedVal, updatedAt, isUpdating) = cachedValWithTs
      val cachedVal = _cachedVal.asInstanceOf[T]

      if (toTs() < updatedAt + ttl) cachedVal // in cache TTL
      else {
        val running = isUpdating.getAndSet(true)

        if (running) cachedVal
        else {
          val value = op
          Future(value)(executionContext) onComplete {
            case Failure(ex) =>
              put(key, cachedVal, false)
              logger.error(s"withCache update failed: $cacheKey", ex)
            case Success(newValue) =>
              put(key, newValue, broadcast = (broadcast && newValue != cachedVal))
              logger.info(s"withCache update success: $cacheKey")
          }

          cachedVal
        }
      }
    }
  }

  def invalidateAll() = cache.invalidateAll()

  def asMap() = cache.asMap()

  def get(key: String) = cache.asMap().get(toCacheKey(key))

  def toFile(file: File): Unit = {
    import org.apache.hadoop.io.WritableUtils
    val output = new DataOutputStream(new FileOutputStream(file))
    try {
      val m = cache.asMap()
      val size = m.size()

      WritableUtils.writeVInt(output, size)
      for (key <- m.keys) {
        val (value, _, _) = m.get(key)
        WritableUtils.writeVLong(output, key)
        serialise(value).foreach { sv =>
          WritableUtils.writeCompressedByteArray(output, sv)
        }
      }
    } finally {
      output.close()
    }
  }

  def fromFile(file: File): Unit = {
    import org.apache.hadoop.io.WritableUtils
    val input = new DataInputStream(new FileInputStream(file))
    try {
      val size = WritableUtils.readVInt(input)
      (1 to size).foreach { ith =>
        val cacheKey = WritableUtils.readVLong(input)
        val value = deserialise(WritableUtils.readCompressedByteArray(input))
        value.foreach { dsv =>
          putInner(cacheKey, dsv)
        }
      }
    } finally {
      input.close()
    }
  }

  def shutdown() = synchronizerOpt.foreach(_.shutdown())
}

