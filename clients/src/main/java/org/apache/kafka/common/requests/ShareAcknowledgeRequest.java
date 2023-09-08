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

import org.apache.kafka.clients.ShareSessionHandler;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ShareAcknowledgeRequest extends AbstractRequest {
  private final ShareAcknowledgeRequestData data;

  public static final class AcknowledgementBatch {
    public final long startOffset;
    public final long lastOffset;
    public final AcknowledgeType type;

    public AcknowledgementBatch(
            long startOffset,
            long lastOffset,
            AcknowledgeType type
    ) {
      this.startOffset = startOffset;
      this.lastOffset = lastOffset;
      this.type = type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ShareAcknowledgeRequest.AcknowledgementBatch that = (ShareAcknowledgeRequest.AcknowledgementBatch) o;
      return startOffset == that.startOffset &&
              lastOffset == that.lastOffset &&
              type == that.type;
    }

    @Override
    public int hashCode() {
      return Objects.hash(startOffset, lastOffset, type);
    }

    @Override
    public String toString() {
      return "AcknowledgementBatch(" +
              "startOffset=" + startOffset +
              ", lastOffset=" + lastOffset +
              ", type=" + type +
              ')';
    }
  }

  public static class Builder extends AbstractRequest.Builder<ShareAcknowledgeRequest> {

    private final ShareAcknowledgeRequestData data;

    public Builder(ShareAcknowledgeRequestData data) {
      super(ApiKeys.SHARE_ACKNOWLEDGE);
      this.data = data;
    }

    public static Builder forConsumer(Map<TopicIdPartition, List<AcknowledgementBatch>> ackData) {
      ShareAcknowledgeRequestData data = new ShareAcknowledgeRequestData();

      data.setTopics(new ArrayList<>());
      ShareAcknowledgeRequestData.AcknowledgeTopic ackTopic = null;
      for (Map.Entry<TopicIdPartition, List<AcknowledgementBatch>> entry : ackData.entrySet()) {
        TopicIdPartition topicPartition = entry.getKey();
        List<AcknowledgementBatch> ackBatches = entry.getValue();

        if (ackTopic == null || !topicPartition.topicId().equals(ackTopic.topicId())) {
          ackTopic = new ShareAcknowledgeRequestData.AcknowledgeTopic()
                  .setTopicId(topicPartition.topicId())
                  .setPartitions(new ArrayList<>());
          data.topics().add(ackTopic);
        }

        ShareAcknowledgeRequestData.AcknowledgePartition ackPartition = new ShareAcknowledgeRequestData.AcknowledgePartition()
                .setPartitionIndex(topicPartition.partition())
                .setAcknowledgementBatches(new ArrayList<>());
        ackTopic.partitions().add(ackPartition);

        for (AcknowledgementBatch batch : ackBatches) {
          ShareAcknowledgeRequestData.AcknowledgementBatch ackBatch =
                  new ShareAcknowledgeRequestData.AcknowledgementBatch()
                          .setStartOffset(batch.startOffset)
                          .setLastOffset(batch.lastOffset)
                          .setGapOffsets(new ArrayList<>())
                          .setAcknowledgeType(batch.type.toString());
          ackPartition.acknowledgementBatches().add(ackBatch);
        }
      }

      return new Builder(data);
    }

    public Builder forShareSession(FetchMetadata metadata) {
      data.setSessionId(metadata.sessionId());
      data.setSessionEpoch(metadata.epoch());
      return this;
    }

    @Override
    public ShareAcknowledgeRequest build(short version) {
      return new ShareAcknowledgeRequest(data, version);
    }
  }

  public ShareAcknowledgeRequest(ShareAcknowledgeRequestData shareAcknowledgeRequestData, short version) {
    super(ApiKeys.SHARE_ACKNOWLEDGE, version);
    this.data = shareAcknowledgeRequestData;
  }

  @Override
  public ShareAcknowledgeRequestData data() { return data; }

  @Override
  public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
    Errors error = Errors.forException(e);
    return new ShareAcknowledgeResponse(new ShareAcknowledgeResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code()));
  }

  public static ShareAcknowledgeRequest parse(ByteBuffer buffer, short version) {
    return new ShareAcknowledgeRequest(new ShareAcknowledgeRequestData(new ByteBufferAccessor(buffer), version), version);
  }
}
