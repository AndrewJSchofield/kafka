/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.server.QuotaFactory.UnboundedQuota
import kafka.utils._
import org.apache.kafka.clients.consumer.AcknowledgeType
import org.apache.kafka.common.message.{ShareAcknowledgeRequestData, ShareFetchResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchMetadata.{FINAL_EPOCH, INITIAL_EPOCH, INVALID_SESSION_ID}
import org.apache.kafka.common.requests.ShareFetchRequest.PartitionData
import org.apache.kafka.common.requests.{FetchMetadata, FetchRequest, ShareFetchRequest, ShareFetchResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.{FetchParams, FetchPartitionData}

import java.util
import java.util.{Optional, OptionalInt, OptionalLong}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CompletableFuture, ThreadLocalRandom}
import scala.collection.{Seq, mutable}

object ShareManager {

}

sealed trait ShareDeliveryState
case object AVAILABLE extends ShareDeliveryState
case object ACQUIRED extends ShareDeliveryState
case object ACKNOWLEDGED extends ShareDeliveryState
case object ARCHIVED extends ShareDeliveryState

class ShareManager(val config: KafkaConfig,
                   metrics: Metrics,
                   time: Time,
                   scheduler: Scheduler,
                   val metadataCache: MetadataCache,
                   val replicaManager: ReplicaManager,
                   val isShuttingDown: AtomicBoolean = new AtomicBoolean(false),
                  ) extends Logging {

  // A map of topic ID to SharePartition.
  private val sharePartitions = new mutable.HashMap[SharePartitionKey, SharePartition]

  // A map of session ID to ShareSession.
  private val sessions = new mutable.HashMap[Int, ShareSession]

  /**
   * Create or update a context for fetching acquired records from share-partitions
   */
  def getSession(time: Time, groupId: String, memberId: String, reqMetadata: FetchMetadata, request: ShareFetchRequest): ShareSession = {
    if (reqMetadata.sessionId == INVALID_SESSION_ID) {
      synchronized {
        val session = new ShareSession(groupId, memberId, newSessionId(),
          FetchMetadata.nextEpoch(INITIAL_EPOCH), time.milliseconds(), time.milliseconds())
        session.initialize(this, request, metadataCache.topicIdsToNames())
        sessions.put(session.sessionId, session)
        session
      }
    } else synchronized {
      sessions.get(reqMetadata.sessionId) match {
        case None =>
          throw Errors.FETCH_SESSION_ID_NOT_FOUND.exception()
        case Some(session) =>
          if (reqMetadata.epoch == FINAL_EPOCH) {
            sessions.remove(reqMetadata.sessionId)
            session
          } else if (reqMetadata.epoch == INITIAL_EPOCH) {
            session.resetEpoch()
            session.update(this, request, metadataCache.topicIdsToNames(), time.milliseconds())
            session
          } else if (session.sessionEpoch != reqMetadata.epoch) {
            throw Errors.INVALID_FETCH_SESSION_EPOCH.exception()
          } else {
            session.update(this, request, metadataCache.topicIdsToNames(), time.milliseconds())
            session
          }
      }
    }
  }

  def getSession(time: Time, sessionId: Int, epoch: Int) : ShareSession = synchronized {
    sessions.get(sessionId) match {
      case None =>
        throw Errors.FETCH_SESSION_ID_NOT_FOUND.exception()
      case Some(session) =>
        if (epoch == FINAL_EPOCH) {
          sessions.remove(sessionId)
          session
        } else if (epoch == INITIAL_EPOCH) {
          session.resetEpoch()
          session.update(this, time.milliseconds())
          session
        } else if (session.sessionEpoch != epoch) {
          throw Errors.INVALID_FETCH_SESSION_EPOCH.exception()
        } else {
          session.update(this, time.milliseconds())
          session
        }
    }
  }

  def newSessionId(): Int = synchronized {
    var id = 0
    do {
      id = ThreadLocalRandom.current().nextInt(1, Int.MaxValue)
    } while (sessions.contains(id) || id == INVALID_SESSION_ID)
    id
  }

  def getSharePartition(groupId: String,
                        topic: String,
                        topicId: Uuid,
                        partition: Int): SharePartition = {
    val key = new SharePartitionKey(groupId, topicId, partition)
    sharePartitions.getOrElseUpdate(key, new SharePartition(groupId, topic, topicId, partition))
  }

  def fetch(session: ShareSession,
            params: FetchParams,
            partitionInfos: Seq[(TopicIdPartition, PartitionData)]
           ): CompletableFuture[Seq[(TopicIdPartition, (FetchPartitionData, util.List[ShareFetchResponseData.AcquiredRecords]))]] = {
    session.fetch(replicaManager, params, partitionInfos)
  }
}

class ShareSession(val groupId: String,
                   val memberId: String,
                   val sessionId: Int,
                   var sessionEpoch: Int,
                   val creationMs: Long,
                   var lastUsedMs: Long) extends Logging {

  val sharePartitions: util.Map[TopicIdPartition, SharePartition] =
    new util.LinkedHashMap[TopicIdPartition, SharePartition]

  def foreachPartition(request: ShareFetchRequest, fun: (TopicIdPartition, ShareFetchRequest.PartitionData) => Unit): Unit = {
    sharePartitions.forEach((tp, _) => {
      fun(tp, new PartitionData(tp.topicId(), tp.partition(), 1048576, Optional.empty()))
    })
  }

  def acknowledge(tip: TopicIdPartition, ackBatches: util.List[ShareAcknowledgeRequestData.AcknowledgementBatch]) : Boolean = synchronized {
    val sharePartition = sharePartitions.get(tip)
    if (sharePartition != null) {
      sharePartition.acknowledge(ackBatches)
    } else {
      false
    }
  }

  def getResponseSize(updates: util.LinkedHashMap[TopicIdPartition, ShareFetchResponseData.PartitionData], versionId: Short): Int = {
    ShareFetchResponse.sizeOf(versionId, updates.entrySet.iterator)
  }

  def getThrottledResponse(throttleTimeMs: Int): ShareFetchResponse =
    ShareFetchResponse.of(Errors.NONE, throttleTimeMs, sessionId, new util.LinkedHashMap[TopicIdPartition, ShareFetchResponseData.PartitionData])

  def updateAndGenerateResponseData(updates: util.LinkedHashMap[TopicIdPartition, ShareFetchResponseData.PartitionData]): ShareFetchResponse = {
    ShareFetchResponse.of(Errors.NONE, 0, sessionId, updates)
  }

  def initialize(shareManager: ShareManager, request: ShareFetchRequest, topicNames: util.Map[Uuid, String]): Unit = synchronized {
    request.data().topics().forEach { t =>
      t.partitions().forEach { p =>
        val tip = new TopicIdPartition(t.topicId, p.partitionIndex(), topicNames.get(t.topicId))
        if (sharePartitions.get(tip) == null) {
          sharePartitions.put(tip, shareManager.getSharePartition(groupId, topicNames.get(t.topicId), t.topicId, p.partitionIndex()))
        }
      }
    }
    request.data().forgottenTopicsData().forEach { t =>
      t.partitions().forEach { p =>
        val tip = new TopicIdPartition(t.topicId, p, topicNames.get(t.topicId))
        sharePartitions.remove(tip)
      }
    }
  }

  def update(shareManager: ShareManager, request: ShareFetchRequest, topicNames: util.Map[Uuid, String], now: Long): Unit = synchronized {
    request.data().topics().forEach { t =>
      t.partitions().forEach { p =>
        val tip = new TopicIdPartition(t.topicId, p.partitionIndex(), topicNames.get(t.topicId))
        if (sharePartitions.get(tip) == null) {
          sharePartitions.put(tip, shareManager.getSharePartition(groupId, topicNames.get(t.topicId), t.topicId, p.partitionIndex()))
        }
      }
    }
    request.data().forgottenTopicsData().forEach { t =>
      t.partitions().forEach { p =>
        val tip = new TopicIdPartition(t.topicId, p, topicNames.get(t.topicId))
        sharePartitions.remove(tip)
      }
    }
    sessionEpoch = FetchMetadata.nextEpoch(sessionEpoch)
    lastUsedMs = now
  }

  def resetEpoch(): Unit = synchronized {
    sessionEpoch = FetchMetadata.INITIAL_EPOCH
  }

  def update(shareManager: ShareManager, now: Long): Unit = synchronized {
    sessionEpoch = FetchMetadata.nextEpoch(sessionEpoch)
    lastUsedMs = now
  }

  def fetch(replicaManager: ReplicaManager,
            params: FetchParams,
            partitionsInfo: Seq[(TopicIdPartition, PartitionData)]
           ): CompletableFuture[Seq[(TopicIdPartition, (FetchPartitionData, util.List[ShareFetchResponseData.AcquiredRecords]))]] = {
    val future = new CompletableFuture[Seq[(TopicIdPartition, (FetchPartitionData, util.List[ShareFetchResponseData.AcquiredRecords]))]]()
    val fetchResponseData = new mutable.ArrayBuffer[(TopicIdPartition, (FetchPartitionData, util.List[ShareFetchResponseData.AcquiredRecords]))]

    def fetchResponseCallback(responsePartitionData: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      responsePartitionData.foreach { case (topicIdPartition, data) =>
        val sharePartition = sharePartitions.get(topicIdPartition)
        fetchResponseData += topicIdPartition -> sharePartition.completeFetch(data)
      }
      future.complete(fetchResponseData)
    }

    val fetchInfos = new mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]
    partitionsInfo.foreach { case (topicIdPartition, partInfo) =>
      val sharePartition = sharePartitions.get(topicIdPartition)
      val ((recordBatch, acquiredRecords), fetchInfo) = sharePartition.beginFetch(partInfo.maxBytes, partInfo.currentLeaderEpoch)
      if (recordBatch != null) {
        fetchResponseData += topicIdPartition -> (recordBatch, acquiredRecords)
      } else {
        fetchInfos += topicIdPartition -> fetchInfo
      }
    }

    if (fetchInfos.nonEmpty) {
      replicaManager.fetchMessages(
        params,
        fetchInfos,
        UnboundedQuota,
        fetchResponseCallback
      )
    } else {
      future.complete(fetchResponseData)
    }

    future
  }
}

class SharePartitionCachedRecord(val offset: Long,
                                 var batch: FetchPartitionData) {
  val MAX_DELIVERY_COUNT = 5
  var deliveryState: ShareDeliveryState = AVAILABLE
  var deliveryCount = 0

  def acquire(): Unit = {
    deliveryCount += 1
    deliveryState = ACQUIRED
  }

  def release(): Boolean = {
    if (deliveryCount >= MAX_DELIVERY_COUNT) {
      deliveryState = ARCHIVED
      batch = null
      true
    } else {
      deliveryState = AVAILABLE
      false
    }
  }

  def reject(): Unit = {
    deliveryState = ARCHIVED
    batch = null
  }

  def acknowledge(): Unit = {
    deliveryState = ACKNOWLEDGED
    batch = null
  }

  def isAvailable: Boolean = {
    deliveryState == AVAILABLE
  }

  def isAcquired: Boolean = {
    deliveryState == ACQUIRED
  }

  def canAdvancePast: Boolean = {
    (deliveryState == ACKNOWLEDGED) || (deliveryState == ARCHIVED)
  }
}

class SharePartitionKey(val groupId: String,
                        val topicId: Uuid,
                        val partition: Int) {

  override def equals(that: Any): Boolean = that match {
    case other: SharePartitionKey => groupId.equals(other.groupId) && topicId.equals(other.topicId) && partition == other.partition
    case _ => false
  }

  override def hashCode(): Int = {
    topicId.hashCode() + partition
  }
}

class SharePartition(val groupId: String,
                     val topic: String,
                     val topicId: Uuid,
                     val partition: Int
                    ) extends Logging {
  // Share-Partition Start Offset
  var startOffset: Long = 0
  // Share-Partition End Offset
  var endOffset: Long = 0
  // The number of available records
  var availableRecordCount = 0
  // The number of in-flight records
  var inFlightRecordCount = 0
  // This can be exceeded slightly by up to one bunch of RecordBatch
  val MAX_IN_FLIGHT_RECORDS = 200
  // A map of the delivery states of the in-flight records - ripe for optimisation later
  var recordCache = new util.concurrent.ConcurrentSkipListMap[Long, SharePartitionCachedRecord]()

  // The first stage of fetch records from a share-partition. Can return a batch of acquired records
  // or the information to begin a fetch from the replica manager
  def beginFetch(maxBytes: Int, currentLeaderEpoch: Optional[Integer]): ((FetchPartitionData, util.List[ShareFetchResponseData.AcquiredRecords]), FetchRequest.PartitionData) = synchronized {
    val acquiredRecords = new util.LinkedList[ShareFetchResponseData.AcquiredRecords]

    // Check whether there are any available records, in which case just return the batch in hand
    if (availableRecordCount > 0) {
      var fetchPartitionData: FetchPartitionData = null
      // Find the first available record
      for (offset <- startOffset until endOffset) {
        val cachedRecord = recordCache.get(offset)
        if ((fetchPartitionData == null) && cachedRecord.isAvailable) {
          fetchPartitionData = cachedRecord.batch
        }
      }

      if (fetchPartitionData != null) {
        fetchPartitionData.records.batches().forEach { batch =>
          var acquired: ShareFetchResponseData.AcquiredRecords = null
          for (offset <- batch.baseOffset() until batch.nextOffset()) {
            val cachedRecord = recordCache.get(offset)
            if (cachedRecord.isAvailable) {
              cachedRecord.acquire()
              availableRecordCount -= 1
              val deliveryCount = cachedRecord.deliveryCount.toShort
              if (acquired == null) {
                acquired = new ShareFetchResponseData.AcquiredRecords()
                acquired.setBaseOffset(offset)
                acquired.setDeliveryCount(deliveryCount)
              } else if ((offset != acquired.lastOffset() + 1) ||
                (deliveryCount != acquired.deliveryCount())) {
                acquiredRecords.add(acquired)

                acquired = new ShareFetchResponseData.AcquiredRecords()
                acquired.setBaseOffset(offset)
                acquired.setDeliveryCount(deliveryCount)
              } else {
              }
              acquired.setLastOffset(offset)
            } else {
            }
          }
          if (acquired != null) {
            acquiredRecords.add(acquired)
          }
        }

        return ((fetchPartitionData, acquiredRecords), null)
      }
    }

    // First, check whether the number of in-flight records reached the limit
    if (inFlightRecordCount >= MAX_IN_FLIGHT_RECORDS) {
      tidyRecordCache()
      return ((new FetchPartitionData(Errors.NONE,
        0,
        0,
        MemoryRecords.EMPTY,
        Optional.empty(),
        OptionalLong.of(0),
        Optional.empty(),
        OptionalInt.empty,
        false), acquiredRecords), null)
    }

    val fetchInfo = new FetchRequest.PartitionData(
      topicId,
      endOffset,
      FetchRequest.INVALID_LOG_START_OFFSET,
      maxBytes,
      currentLeaderEpoch
    )
    ((null, null), fetchInfo)
  }

  // The second stage of fetching records from a share-partition. This adds fetched records to the
  // cache and returns a batch of them acquired for the caller.
  // Does not handle log compaction, control records. Assumes gapless offsets.
  def completeFetch(fetchPartitionData: FetchPartitionData): (FetchPartitionData, util.List[ShareFetchResponseData.AcquiredRecords]) = synchronized {
    if (fetchPartitionData != null) {
      fetchPartitionData.records.batches().forEach { batch =>
        val baseOffset = batch.baseOffset()
        val nextOffset = batch.nextOffset()
        for (offset <- baseOffset until nextOffset) {
          val cachedRecord = recordCache.get(offset)
          if (cachedRecord == null) {
            recordCache.put(offset, new SharePartitionCachedRecord(offset, fetchPartitionData))
            availableRecordCount += 1
            inFlightRecordCount += 1
            if (offset >= endOffset) endOffset += 1
          }
        }
      }
    }

    var cachedPartitionData: FetchPartitionData = null
    val acquiredRecords = new util.LinkedList[ShareFetchResponseData.AcquiredRecords]
    if (availableRecordCount > 0) {
      // Find the first available record
      for (offset <- startOffset until endOffset) {
        val cachedRecord = recordCache.get(offset)
        if ((cachedPartitionData == null) && cachedRecord.isAvailable) {
          cachedPartitionData = cachedRecord.batch
        }
      }

      if (cachedPartitionData != null) {
        var acquired: ShareFetchResponseData.AcquiredRecords = null
        cachedPartitionData.records.batches().forEach { batch =>
          val baseOffset = batch.baseOffset()
          val nextOffset = batch.nextOffset()
          for (offset <- baseOffset until nextOffset) {
            val cachedRecord = recordCache.get(offset)
            if (cachedRecord.isAvailable) {
              cachedRecord.acquire()
              availableRecordCount -= 1
              val deliveryCount = cachedRecord.deliveryCount.toShort

              if (acquired == null) {
                acquired = new ShareFetchResponseData.AcquiredRecords()
                acquired.setBaseOffset(offset)
                acquired.setDeliveryCount(deliveryCount)
              } else if ((offset != acquired.lastOffset() + 1) ||
                (deliveryCount != acquired.deliveryCount())) {
                acquiredRecords.add(acquired)

                acquired = new ShareFetchResponseData.AcquiredRecords()
                acquired.setBaseOffset(offset)
                acquired.setDeliveryCount(deliveryCount)
              }
              acquired.setLastOffset(offset)
            }
          }
        }
        if (acquired != null) acquiredRecords.add(acquired)
      }
    }

    if (cachedPartitionData == null)
      (new FetchPartitionData(Errors.NONE,
        0,
        0,
        MemoryRecords.EMPTY,
        Optional.empty(),
        OptionalLong.of(0),
        Optional.empty(),
        OptionalInt.empty,
        false), acquiredRecords)
    else
      (cachedPartitionData, acquiredRecords)
  }

  def acknowledge(ackBatches: util.List[ShareAcknowledgeRequestData.AcknowledgementBatch]): Boolean = synchronized {
    var success = true

    // Pass 1: validate the requested acknowledgements
    var batchIterator = ackBatches.iterator()
    while (batchIterator.hasNext) {
      val batch = batchIterator.next()
      for (offset <- batch.startOffset() to batch.lastOffset()) {
        val cachedRecord = recordCache.get(offset)
        if ((cachedRecord == null) || !cachedRecord.isAcquired)
          success = false
      }
      batch.gapOffsets().forEach(offset => {
        val cachedRecord = recordCache.get(offset)
        if ((cachedRecord != null) && !cachedRecord.isAcquired)
          success = false
      })
    }

    // Pass 2: perform the requested acknowledgements
    if (success) {
      var tidy = false
      batchIterator = ackBatches.iterator()
      while (batchIterator.hasNext) {
        val batch = batchIterator.next()
        for (offset <- batch.startOffset() to batch.lastOffset()) {
          val cachedRecord = recordCache.get(offset)
          val ackType = AcknowledgeType.fromString(batch.acknowledgeType())
          ackType match {
            case AcknowledgeType.ACCEPT =>
              cachedRecord.acknowledge()
              tidy = true
            case AcknowledgeType.RELEASE =>
              if (cachedRecord.release())
                tidy = true
              else
                availableRecordCount += 1
            case AcknowledgeType.REJECT =>
              cachedRecord.reject()
              tidy = true
          }
        }
        batch.gapOffsets().forEach(offset => {
          val cachedRecord = recordCache.get(offset)
          if (cachedRecord != null) {
            recordCache.remove(offset)
            if (cachedRecord.isAvailable)
              availableRecordCount -= 1
            inFlightRecordCount -= 1
          }
        })
      }

      if (tidy)
        tidyRecordCache()
    }

    success
  }

  // This scans the record cache from the lowest offset removing records until the
  // first record that is available or acquired. This keeps control of memory usage
  // and moves the SPSO forwards too.
  def tidyRecordCache(): Unit = synchronized {
    var newOffset = startOffset
    val keySet = recordCache.navigableKeySet()
    val it = keySet.iterator()
    var done = false
    while (!done && it.hasNext) {
      val thisOffset = it.next()
      val cachedRecord = recordCache.get(thisOffset)
      if (!cachedRecord.canAdvancePast) {
        newOffset = thisOffset
        done = true
      } else {
        newOffset = thisOffset + 1
        inFlightRecordCount -= 1
        it.remove()
      }
    }

    if (newOffset > startOffset) {
      startOffset = newOffset
    }
  }
}