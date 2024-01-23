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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.List;

public class ShareFetchCollector<K, V> {
  private final Logger log;
  private final FetchConfig fetchConfig;
  private final Deserializers<K,V> deserializers;

  public ShareFetchCollector(final LogContext logContext,
                             final FetchConfig fetchConfig,
                             final Deserializers<K, V> deserializers) {
    this.log = logContext.logger(ShareFetchCollector.class);
    this.fetchConfig = fetchConfig;
    this.deserializers = deserializers;
  }

  public Fetch<K, V> collectFetch(final ShareFetchBuffer fetchBuffer) {
    final Fetch<K, V> fetch = Fetch.empty();
    int recordsRemaining = fetchConfig.maxPollRecords;

    try {
      while (recordsRemaining > 0) {
        final CompletedShareFetchNew nextInLineFetch = fetchBuffer.nextInLineFetch();

        if (nextInLineFetch == null || nextInLineFetch.isConsumed()) {
          final CompletedShareFetchNew completedFetch = fetchBuffer.peek();

          if (completedFetch == null) {
            break;
          }

          if (!completedFetch.isInitialized()) {
            try {
              fetchBuffer.setNextInLineFetch(completedFetch);
            } catch (Exception e) {
              if (fetch.isEmpty() && ShareFetchResponse.recordsOrFail(completedFetch.partitionData).sizeInBytes() == 0)
                fetchBuffer.poll();

              throw e;
            }
          } else {
            fetchBuffer.setNextInLineFetch(completedFetch);
          }

          fetchBuffer.poll();
        } else {
          final Fetch<K, V> nextFetch = fetchRecords(nextInLineFetch, recordsRemaining);
          recordsRemaining -= nextFetch.numRecords();
          fetch.add(nextFetch);
        }
      }
    } catch (KafkaException e) {
      if (fetch.isEmpty())
        throw e;
    }

    return fetch;
  }

  private Fetch<K, V> fetchRecords(final CompletedShareFetchNew nextInLineFetch, int maxRecords) {
    final TopicIdPartition tp = nextInLineFetch.partition;

    List<ConsumerRecord<K, V>> partRecords = nextInLineFetch.fetchRecords(fetchConfig, deserializers, maxRecords);

    return Fetch.forPartition(tp.topicPartition(), partRecords, true);
  }
}