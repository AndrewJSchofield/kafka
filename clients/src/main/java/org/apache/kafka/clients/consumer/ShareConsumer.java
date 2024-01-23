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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @see KafkaShareConsumer
 */
public interface ShareConsumer<K, V> extends Closeable {

  /**
   * @See KafkaShareConsumer#acknowledge(ConsumerRecord)
   */
  void acknowledge(ConsumerRecord record);

  /**
   * @See KafkaShareConsumer#acknowledge(ConsumerRecord, AcknowledgementType)
   */
  void acknowledge(ConsumerRecord record, AcknowledgeType type);

  /**
   * @see KafkaShareConsumer#subscription()
   */
  Set<String> subscription();

  /**
   * @see KafkaShareConsumer#subscribe(Collection)
   */
  void subscribe(Collection<String> topics);

  /**
   * @see KafkaShareConsumer#unsubscribe()
   */
  void unsubscribe();

  /**
   * @see KafkaShareConsumer#poll(Duration)
   */
  ConsumerRecords<K, V> poll(Duration timeout);

  /**
   * @see KafkaShareConsumer#commitSync()
   */
  void commitSync();

  /**
   * @see KafkaShareConsumer#commitSync(Duration)
   */
  void commitSync(Duration timeout);

  /**
   * @see KafkaShareConsumer#metrics()
   */
  Map<MetricName, ? extends Metric> metrics();

  /**
   * @see KafkaShareConsumer#close()
   */
  void close();

  /**
   * @see KafkaShareConsumer#close(Duration)
   */
  void close(Duration timeout);

  /**
   * @see KafkaShareConsumer#wakeup()
   */
  void wakeup();

}
