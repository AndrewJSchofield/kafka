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

import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.*;

public class ShareFetchRequest extends AbstractRequest {
  private final ShareFetchRequestData data;
  private volatile List<PartitionData> fetchData = null;
  private volatile List<TopicIdPartition> toForget = null;

  // This is an immutable read-only structure derived from ShareFetchRequestData
  private final FetchMetadata metadata;

  public static final class PartitionData {
    public final Uuid topicId;
    public final int partitionIndex;
    public final int maxBytes;
    public final Optional<Integer> currentLeaderEpoch;

    public PartitionData(
            Uuid topicId,
            int partitionIndex,
            int maxBytes,
            Optional<Integer> currentLeaderEpoch
    ) {
      this.topicId = topicId;
      this.partitionIndex = partitionIndex;
      this.maxBytes = maxBytes;
      this.currentLeaderEpoch = currentLeaderEpoch;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ShareFetchRequest.PartitionData that = (ShareFetchRequest.PartitionData) o;
      return partitionIndex == that.partitionIndex &&
              maxBytes == that.maxBytes &&
              Objects.equals(currentLeaderEpoch, that.currentLeaderEpoch);
    }

    @Override
    public int hashCode() {
      return Objects.hash(partitionIndex, maxBytes, currentLeaderEpoch);
    }

    @Override
    public String toString() {
      return "PartitionData(" +
              "partitionIndex=" + partitionIndex +
              ", maxBytes=" + maxBytes +
              ", currentLeaderEpoch=" + currentLeaderEpoch +
              ')';
    }
  }

  public static class Builder extends AbstractRequest.Builder<ShareFetchRequest> {

    private final ShareFetchRequestData data;

    public Builder(ShareFetchRequestData data) {
      super(ApiKeys.SHARE_FETCH);
      this.data = data;
    }

    public static Builder forConsumer(short maxVersion, int maxWait, int minBytes, Map<TopicIdPartition, PartitionData> fetchData) {
      ShareFetchRequestData data = new ShareFetchRequestData();
      data.setMaxWaitMs(maxWait);
      data.setMinBytes(minBytes);

      // We collect the partitions in a single FetchTopic only if they appear sequentially in the fetchData
      data.setTopics(new ArrayList<>());
      ShareFetchRequestData.FetchTopic fetchTopic = null;
      for (Map.Entry<TopicIdPartition, ShareFetchRequest.PartitionData> entry : fetchData.entrySet()) {
        TopicIdPartition topicPartition = entry.getKey();
        ShareFetchRequest.PartitionData partitionData = entry.getValue();

        if (fetchTopic == null || !topicPartition.topicId().equals(fetchTopic.topicId())) {
          fetchTopic = new ShareFetchRequestData.FetchTopic()
                  .setTopicId(topicPartition.topicId())
                  .setPartitions(new ArrayList<>());
          data.topics().add(fetchTopic);
        }

        ShareFetchRequestData.FetchPartition fetchPartition = new ShareFetchRequestData.FetchPartition()
                .setPartitionIndex(topicPartition.partition())
                .setCurrentLeaderEpoch(partitionData.currentLeaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                .setPartitionMaxBytes(partitionData.maxBytes);

        fetchTopic.partitions().add(fetchPartition);
      }

      return new Builder(data);
    }

    public Builder setMaxBytes(int maxBytes) {
      data.setMaxBytes(maxBytes);
      return this;
    }

    public Builder forShareSession(String groupId, SubscriptionState subscriptions, FetchMetadata metadata) {
      if (metadata != null) {
        data.setSessionId(metadata.sessionId());
        data.setSessionEpoch(metadata.epoch());
        if (metadata.sessionId() == FetchMetadata.INVALID_SESSION_ID) {
          data.setGroupId(groupId);
          data.setMemberId(subscriptions.memberId());
        }
      } else {
        data.setGroupId(groupId);
        data.setMemberId(subscriptions.memberId());
      }
      return this;
    }

    public Builder removed(List<TopicIdPartition> removed) {
      return this;
    }

    public Builder replaced(List<TopicIdPartition> replaced) {
      return this;
    }

    @Override
    public ShareFetchRequest build(short version) {
      return new ShareFetchRequest(data, version);
    }

    @Override
    public String toString() { return data.toString(); }
  }

  public ShareFetchRequest(ShareFetchRequestData shareFetchRequestData, short version) {
    super(ApiKeys.SHARE_FETCH, version);
    this.data = shareFetchRequestData;
    this.metadata = new FetchMetadata(shareFetchRequestData.sessionId(), shareFetchRequestData.sessionEpoch());
  }

  public FetchMetadata metadata() {
    return metadata;
  }

  public List<PartitionData> fetchData() {
    if (fetchData == null) {
      synchronized (this) {
        if (fetchData == null) {
          final LinkedList<PartitionData> fetchDataTmp = new LinkedList<>();
          data.topics().forEach(fetchTopic -> {
            fetchTopic.partitions().forEach(fetchPartition -> {
              fetchDataTmp.add(
                      new PartitionData(fetchTopic.topicId(),
                           fetchPartition.partitionIndex(),
                           fetchPartition.partitionMaxBytes(),
                           Optional.of(fetchPartition.currentLeaderEpoch())
                      ));
            });
          });
          fetchData = fetchDataTmp;
        }
      }
    }
    return fetchData;
  }

  @Override
  public ShareFetchRequestData data() { return data; }

  @Override
  public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
    Errors error = Errors.forException(e);
    List<ShareFetchResponseData.ShareFetchableTopicResponse> topicResponseList = new ArrayList<>();
    return new ShareFetchResponse(new ShareFetchResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code())
            .setSessionId(data.sessionId())
            .setResponses(topicResponseList));
  }

  public static ShareFetchRequest parse(ByteBuffer buffer, short version) {
    return new ShareFetchRequest(new ShareFetchRequestData(new ByteBufferAccessor(buffer), version), version);
  }
}
