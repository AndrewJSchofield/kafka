/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

public class ShareFetchResponse extends AbstractResponse {
  private final ShareFetchResponseData data;
  private volatile LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData = null;

  @Override
  public ShareFetchResponseData data() { return data; }

  public ShareFetchResponse(ShareFetchResponseData shareFetchResponseData) {
    super(ApiKeys.SHARE_FETCH);
    this.data = shareFetchResponseData;
  }

  public LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData(Map<Uuid, String> topicNames, short version) {
    if (responseData == null) {
      synchronized (this) {
        if (responseData == null) {
          final LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> responseDataTmp =
                  new LinkedHashMap<>();
          data.responses().forEach(topicResponse -> {
            String name = topicNames.get(topicResponse.topicId());
            if (name != null) {
              topicResponse.partitions().forEach(partition ->
                      responseDataTmp.put(new TopicIdPartition(topicResponse.topicId(), partition.partitionIndex(), name), partition));
            }
          });
          responseData = responseDataTmp;
        }
      }
    }
    return responseData;
  }

  public Errors error() { return Errors.forCode(data.errorCode()); }

  @Override
  public Map<Errors, Integer> errorCounts() {
    Map<Errors, Integer> errorCounts = new HashMap<>();
    updateErrorCounts(errorCounts, error());
    data.responses().forEach(topicResponse ->
            topicResponse.partitions().forEach(partition ->
                    updateErrorCounts(errorCounts, Errors.forCode(partition.errorCode())))
    );
    return errorCounts;
  }

  public static ShareFetchResponse parse(ByteBuffer buffer, short version) {
    return new ShareFetchResponse(new ShareFetchResponseData(new ByteBufferAccessor(buffer), version));
  }

  @Override
  public int throttleTimeMs() { return data.throttleTimeMs(); }

  @Override
  public void maybeSetThrottleTimeMs(int throttleTimeMs) { data.setThrottleTimeMs(throttleTimeMs); }

  public int sessionId() { return data.sessionId(); }

  public Set<Uuid> topicIds() {
    return data.responses().stream().map(ShareFetchResponseData.ShareFetchableTopicResponse::topicId).filter(id -> !id.equals(Uuid.ZERO_UUID)).collect(Collectors.toSet());
  }

  public static ShareFetchResponseData.PartitionData partitionResponse(TopicIdPartition topicIdPartition, Errors error) {
    return partitionResponse(topicIdPartition.topicPartition().partition(), error);
  }

  public static ShareFetchResponseData.PartitionData partitionResponse(int partition, Errors error) {
    return new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(partition)
            .setErrorCode(error.code())
            .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK);
  }

  public static Records recordsOrFail(ShareFetchResponseData.PartitionData partition) {
    if (partition.records() == null) return MemoryRecords.EMPTY;
    if (partition.records() instanceof Records) return (Records) partition.records();
    throw new ClassCastException("The record type is " + partition.records().getClass().getSimpleName() + ", which is not a subtype of " +
            Records.class.getSimpleName() + ". This method is only safe to call if the `ShareFetchResponse` was deserialized from bytes.");
  }

  /**
   * @return The size in bytes of the records. 0 is returned if records of input partition is null.
   */
  public static int recordsSize(ShareFetchResponseData.PartitionData partition) {
    return partition.records() == null ? 0 : partition.records().sizeInBytes();
  }

  public static ShareFetchResponse of(Errors error,
                                      int throttleTimeMs,
                                      int sessionId,
                                      LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData) {
    return new ShareFetchResponse(toMessage(error, throttleTimeMs, sessionId, responseData.entrySet().iterator()));
  }

  private static boolean matchingTopic(ShareFetchResponseData.ShareFetchableTopicResponse previousTopic, TopicIdPartition currentTopic) {
    if (previousTopic == null)
      return false;
    return previousTopic.topicId().equals(currentTopic.topicId());
  }

  public static int sizeOf(short version,
                           Iterator<Map.Entry<TopicIdPartition,
                                   ShareFetchResponseData.PartitionData>> partIterator) {
    ShareFetchResponseData data = toMessage(Errors.NONE, 0, INVALID_SESSION_ID, partIterator);
    ObjectSerializationCache cache = new ObjectSerializationCache();
    return 4 + data.size(cache, version);
  }

  private static ShareFetchResponseData toMessage(Errors error,
                                                  int throttleTimeMs,
                                                  int sessionId,
                                                  Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> partIterator) {
    List<ShareFetchResponseData.ShareFetchableTopicResponse> topicResponseList = new ArrayList<>();
    while (partIterator.hasNext()) {
      Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> entry = partIterator.next();
      ShareFetchResponseData.PartitionData partitionData = entry.getValue();
      partitionData.setPartitionIndex(entry.getKey().partition());

      // We have to keep the order of input topic-partition. Hence, we batch the partitions only if the last
      // batch is in the same topic group.
      ShareFetchResponseData.ShareFetchableTopicResponse previousTopic = topicResponseList.isEmpty() ? null
              : topicResponseList.get(topicResponseList.size() - 1);
      if (matchingTopic(previousTopic, entry.getKey()))
        previousTopic.partitions().add(partitionData);
      else {
        List<ShareFetchResponseData.PartitionData> partitionResponses = new ArrayList<>();
        partitionResponses.add(partitionData);
        topicResponseList.add(new ShareFetchResponseData.ShareFetchableTopicResponse()
                .setTopicId(entry.getKey().topicId())
                .setPartitions(partitionResponses));
      }
    }

    return new ShareFetchResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code())
            .setSessionId(sessionId)
            .setResponses(topicResponseList);
  }
}
