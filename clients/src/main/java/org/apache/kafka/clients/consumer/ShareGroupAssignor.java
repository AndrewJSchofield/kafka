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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>The share group assignor assigns all topic-partitions to all members.
 *
 * <p>For example, suppose there are two consumers <code>C0</code> and <code>C1</code>, two topics <code>t0</code> and
 * <code>t1</code>, and each topic has 3 partitions, resulting in partitions <code>t0p0</code>, <code>t0p1</code>,
 * <code>t0p2</code>, <code>t1p0</code>, <code>t1p1</code>, and <code>t1p2</code>.
 *
 * <p>The assignment will be:
 * <ul>
 * <li><code>C0: [t0p0, t0p1, t0p2, t1p0, t1p1, t1p2]</code></li>
 * <li><code>C1: [t0p0, t0p1, t0p2, t1p0, t1p1, t1p2]</code></li>
 * </ul>
 *
 */
public class ShareGroupAssignor extends AbstractPartitionAssignor {
  public static final String SHARE_GROUP_ASSIGNOR_NAME = "sharegroup";
  /**
   * Unique name for this assignor (e.g. "range" or "roundrobin" or "sticky"). Note, this is not required
   * to be the same as the class name specified in {@link ConsumerConfig#PARTITION_ASSIGNMENT_STRATEGY_CONFIG}
   *
   * @return non-null unique name
   */
  @Override
  public String name() {
    return SHARE_GROUP_ASSIGNOR_NAME;
  }

  private Map<String, List<MemberInfo>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
    Map<String, List<MemberInfo>> topicToConsumers = new HashMap<>();
    consumerMetadata.forEach((consumerId, subscription) -> {
      MemberInfo memberInfo = new MemberInfo(consumerId, subscription.groupInstanceId(), subscription.rackId());
      subscription.topics().forEach(topic -> put(topicToConsumers, topic, memberInfo));
    });
    return topicToConsumers;
  }

  /**
   * Perform the group assignment given the partition counts and member subscriptions
   *
   * @param partitionsPerTopic The number of partitions for each subscribed topic. Topics not in metadata will be excluded
   *                           from this map.
   * @param subscriptions      Map from the member id to their respective topic subscription
   * @return Map from each member to the list of partitions assigned to them.
   */
  @Override
  public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
    Map<String, List<TopicPartition>> assignment = new HashMap<>();
    subscriptions.keySet().forEach(memberId -> assignment.put(memberId, new ArrayList<>()));

    Map<String, List<MemberInfo>> consumersPerTopic = consumersPerTopic(subscriptions);
    for (Map.Entry<String, List<MemberInfo>> topicConsumer : consumersPerTopic.entrySet()) {
      String topic = topicConsumer.getKey();
      for (MemberInfo consumer : topicConsumer.getValue()) {
        Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
        if (numPartitionsForTopic != null) {
          assignment.get(consumer.memberId).addAll(AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
        }
      }
    }

    return assignment;
  }
}
