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
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ShareAcknowledgeResponse extends AbstractResponse {
  private final ShareAcknowledgeResponseData data;

  public ShareAcknowledgeResponse(ShareAcknowledgeResponseData shareAcknowledgeResponseData) {
    super(ApiKeys.SHARE_ACKNOWLEDGE);
    this.data = shareAcknowledgeResponseData;
  }

  public Errors error() { return Errors.forCode(data.errorCode()); }

  @Override
  public ShareAcknowledgeResponseData data() { return data; }

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

  public static ShareAcknowledgeResponse parse(ByteBuffer buffer, short version) {
    return new ShareAcknowledgeResponse(new ShareAcknowledgeResponseData(new ByteBufferAccessor(buffer), version));
  }

  @Override
  public int throttleTimeMs() { return data.throttleTimeMs(); }

  @Override
  public void maybeSetThrottleTimeMs(int throttleTimeMs) { data.setThrottleTimeMs(throttleTimeMs); }

  public static ShareAcknowledgeResponse of(Errors error,
                                            int throttleTimeMs,
                                            int sessionId,
                                            LinkedHashMap<TopicIdPartition, Errors> responseData) {
    return new ShareAcknowledgeResponse(toMessage(error, throttleTimeMs, sessionId, responseData.entrySet().iterator()));
  }

  private static boolean matchingTopic(ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse previousTopic, TopicIdPartition currentTopic) {
    if (previousTopic == null)
      return false;
    return previousTopic.topicId().equals(currentTopic.topicId());
  }

  private static ShareAcknowledgeResponseData toMessage(Errors error,
                                                        int throttleTimeMs,
                                                        int sessionId,
                                                        Iterator<Map.Entry<TopicIdPartition, Errors>> partIterator) {
    List<ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse> topicResponseList = new ArrayList<>();
    while (partIterator.hasNext()) {
      Map.Entry<TopicIdPartition, Errors> entry = partIterator.next();
      Errors partitionError = entry.getValue();
      ShareAcknowledgeResponseData.PartitionData partitionData = new ShareAcknowledgeResponseData.PartitionData()
              .setPartitionIndex(entry.getKey().partition())
              .setErrorCode(partitionError.code());

      ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse previousTopic = topicResponseList.isEmpty() ? null
              : topicResponseList.get(topicResponseList.size() - 1);
      if (matchingTopic(previousTopic, entry.getKey()))
        previousTopic.partitions().add(partitionData);
      else {
        List<ShareAcknowledgeResponseData.PartitionData> partitionResponses = new ArrayList<>();
        partitionResponses.add(partitionData);
        topicResponseList.add(new ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse()
                .setTopicId(entry.getKey().topicId())
                .setPartitions(partitionResponses));
      }
    }

    return new ShareAcknowledgeResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code())
            .setSessionId(sessionId)
            .setResponses(topicResponseList);
  }
}
