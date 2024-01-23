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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.ShareSessionHandler;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.Acknowledgements;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;

public class ShareAcknowledgeApplicationEvent extends CompletableApplicationEvent<Void> {

  // It's really not nice having the target node in the event - needs refactoring
  private final Node target;
  private final ShareSessionHandler.ShareAcknowledgeRequestData acknowledgements;

  public ShareAcknowledgeApplicationEvent(
          final Node target,
          final ShareSessionHandler.ShareAcknowledgeRequestData acknowledgements) {
    super(Type.SHARE_ACKNOWLEDGE);
    this.target = target;
    this.acknowledgements = acknowledgements;
  }

  public Node target() {
    return target;
  }

  public ShareSessionHandler.ShareAcknowledgeRequestData acknowledgements() {
    return acknowledgements;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    ShareAcknowledgeApplicationEvent that = (ShareAcknowledgeApplicationEvent) o;

    return target == that.target && acknowledgements.equals(that.acknowledgements);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + target.id();
    result = 31 * result + acknowledgements.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "ShareAcknowledgeApplicationEvent{" +
            toStringBase() +
            ", target=" + target +
            ", acknowledgements=" + acknowledgements +
            '}';
  }
}
