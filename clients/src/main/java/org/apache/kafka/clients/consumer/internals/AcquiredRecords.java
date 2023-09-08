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

import org.apache.kafka.common.message.ShareFetchResponseData;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

/**
 * {@Link AcquiredRecords} represents the acquired records for a {@Link CompletedShareFetch}.
 */
class AcquiredRecords {
  final LinkedHashMap<Long, Short> recordCounts;

  AcquiredRecords(List<ShareFetchResponseData.AcquiredRecords> acquiredRecords) {
    this.recordCounts = new LinkedHashMap<>();
    for (ShareFetchResponseData.AcquiredRecords ar : acquiredRecords) {
      for (long offset = ar.baseOffset(); offset <= ar.lastOffset(); offset++) {
        recordCounts.put(offset, ar.deliveryCount());
      }
    }
  }

  Optional<Short> getDeliveryCount(long offset) {
    Short deliveryCount = recordCounts.get(offset);
    return deliveryCount != null ? Optional.of(deliveryCount) : Optional.empty();
  }
}
