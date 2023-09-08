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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.TopicIdPartition;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public class Acknowledgements {
  private final Map<TopicIdPartition, Map<Long, AcknowledgeType>> acknowledgements;

  public static Acknowledgements empty() { return new Acknowledgements(new HashMap<>()); }

  private Acknowledgements(Map<TopicIdPartition, Map<Long, AcknowledgeType>> acknowledgements) {
    this.acknowledgements = acknowledgements;
  }

  public void add(TopicIdPartition partition,
                  long offset,
                  AcknowledgeType type) {
    Map<Long, AcknowledgeType> partitionAcks = this.acknowledgements.get(partition);
    if (partitionAcks == null) {
      this.acknowledgements.put(partition, mkMap(mkEntry(offset, type)));
    } else {
      partitionAcks.put(offset, type);
    }
  }

  public List<TopicIdPartition> partitions() {
    LinkedList<TopicIdPartition> partitionList = new LinkedList<>();
    partitionList.addAll(acknowledgements.keySet());
    return partitionList;
  }

  public Map<Long, AcknowledgeType> forPartition(TopicIdPartition partition) {
    return acknowledgements.get(partition);
  }

  public String toString() {
    return "[" + acknowledgements
            .entrySet()
            .stream()
            .map(e1 -> e1.getKey() + ":" + e1.getValue()
                    .entrySet()
                    .stream()
                    .map(e2 -> e2.getKey() + "->" + e2.getValue().toString())
                    .collect(Collectors.joining(",")))
            .collect(Collectors.joining(";")) + "]";
  }
}
