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
package org.apache.kafka.coordinator.group.share;

import org.apache.kafka.common.Uuid;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * ShareGroupMember contains all the information related to a member
 * within a share group. This class is immutable.
 */
public class ShareGroupMember {
  /**
   * A builder that facilitates the creation of a new member or the update of
   * an existing one.
   *
   * Please refer to the javadoc of {{@link ShareGroupMember}} for the
   * definition of the fields.
   */
  public static class Builder {
    private final String memberId;
    private int memberEpoch = 0;
    private int previousMemberEpoch = -1;
    private int targetMemberEpoch = 0;
    private String rackId = null;
    private int rebalanceTimeoutMs = -1;
    private String clientId = "";
    private String clientHost = "";
    private List<String> subscribedTopicNames = Collections.emptyList();
    private Map<Uuid, Set<Integer>> assignedPartitions = Collections.emptyMap();

    public Builder(String memberId) {
      this.memberId = Objects.requireNonNull(memberId);
    }

    public Builder(ShareGroupMember member) {
      Objects.requireNonNull(member);

      this.memberId = member.memberId;
      this.memberEpoch = member.memberEpoch;
      this.previousMemberEpoch = member.previousMemberEpoch;
      this.targetMemberEpoch = member.targetMemberEpoch;
      this.rackId = member.rackId;
      this.rebalanceTimeoutMs = member.rebalanceTimeoutMs;
      this.clientId = member.clientId;
      this.clientHost = member.clientHost;
      this.subscribedTopicNames = member.subscribedTopicNames;
      this.assignedPartitions = member.assignedPartitions;
    }

    public Builder setMemberEpoch(int memberEpoch) {
      this.memberEpoch = memberEpoch;
      return this;
    }

    public Builder setPreviousMemberEpoch(int previousMemberEpoch) {
      this.previousMemberEpoch = previousMemberEpoch;
      return this;
    }

    public Builder setTargetMemberEpoch(int targetMemberEpoch) {
      this.targetMemberEpoch = targetMemberEpoch;
      return this;
    }

    public Builder setRackId(String rackId) {
      this.rackId = rackId;
      return this;
    }

    public Builder maybeUpdateRackId(Optional<String> rackId) {
      this.rackId = rackId.orElse(this.rackId);
      return this;
    }

    public Builder setRebalanceTimeoutMs(int rebalanceTimeoutMs) {
      this.rebalanceTimeoutMs = rebalanceTimeoutMs;
      return this;
    }

    public Builder maybeUpdateRebalanceTimeoutMs(OptionalInt rebalanceTimeoutMs) {
      this.rebalanceTimeoutMs = rebalanceTimeoutMs.orElse(this.rebalanceTimeoutMs);
      return this;
    }

    public Builder setClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder setClientHost(String clientHost) {
      this.clientHost = clientHost;
      return this;
    }

    public Builder setSubscribedTopicNames(List<String> subscribedTopicNames) {
      this.subscribedTopicNames = subscribedTopicNames;
      this.subscribedTopicNames.sort(Comparator.naturalOrder());
      return this;
    }

    public Builder maybeUpdateSubscribedTopicNames(Optional<List<String>> subscribedTopicNames) {
      this.subscribedTopicNames = subscribedTopicNames.orElse(this.subscribedTopicNames);
      this.subscribedTopicNames.sort(Comparator.naturalOrder());
      return this;
    }

    public Builder setAssignedPartitions(Map<Uuid, Set<Integer>> assignedPartitions) {
      this.assignedPartitions = assignedPartitions;
      return this;
    }

    public ShareGroupMember build() {
      return new ShareGroupMember(
              memberId,
              memberEpoch,
              previousMemberEpoch,
              targetMemberEpoch,
              rackId,
              rebalanceTimeoutMs,
              clientId,
              clientHost,
              subscribedTopicNames,
              MemberState.STABLE,
              assignedPartitions
      );
    }
  }

  /**
   * The various states that a member can be in. For their definition,
   * refer to the documentation of {{@link CurrentAssignmentBuilder}}.
   */
  public enum MemberState {
    REVOKING("revoking"),
    ASSIGNING("assigning"),
    STABLE("stable");

    private final String name;

    MemberState(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * The member id.
   */
  private final String memberId;

  /**
   * The current member epoch.
   */
  private final int memberEpoch;

  /**
   * The previous member epoch.
   */
  private final int previousMemberEpoch;

  /**
   * The next member epoch. This corresponds to the target
   * assignment epoch used to compute the current assigned,
   * revoking and assigning partitions.
   */
  private final int targetMemberEpoch;

  /**
   * The rack id provided by the member.
   */
  private final String rackId;

  /**
   * The rebalance timeout provided by the member.
   */
  private final int rebalanceTimeoutMs;

  /**
   * The client id reported by the member.
   */
  private final String clientId;

  /**
   * The host reported by the member.
   */
  private final String clientHost;

  /**
   * The list of subscriptions (topic names) configured by the member.
   */
  private final List<String> subscribedTopicNames;

  /**
   * The member state.
   */
  private final MemberState state;

  /**
   * The partitions assigned to this member.
   */
  private final Map<Uuid, Set<Integer>> assignedPartitions;

  private ShareGroupMember(
          String memberId,
          int memberEpoch,
          int previousMemberEpoch,
          int targetMemberEpoch,
          String rackId,
          int rebalanceTimeoutMs,
          String clientId,
          String clientHost,
          List<String> subscribedTopicNames,
          MemberState state,
          Map<Uuid, Set<Integer>> assignedPartitions
  ) {
    this.memberId = memberId;
    this.memberEpoch = memberEpoch;
    this.previousMemberEpoch = previousMemberEpoch;
    this.targetMemberEpoch = targetMemberEpoch;
    this.rackId = rackId;
    this.rebalanceTimeoutMs = rebalanceTimeoutMs;
    this.clientId = clientId;
    this.clientHost = clientHost;
    this.subscribedTopicNames = subscribedTopicNames;
    this.state = state;
    this.assignedPartitions = assignedPartitions;
  }

  /**
   * @return The member id.
   */
  public String memberId() {
    return memberId;
  }

  /**
   * @return The current member epoch.
   */
  public int memberEpoch() {
    return memberEpoch;
  }

  /**
   * @return The previous member epoch.
   */
  public int previousMemberEpoch() {
    return previousMemberEpoch;
  }

  /**
   * @return The target member epoch.
   */
  public int targetMemberEpoch() {
    return targetMemberEpoch;
  }

  /**
   * @return The rack id.
   */
  public String rackId() {
    return rackId;
  }

  /**
   * @return The rebalance timeout in millis.
   */
  public int rebalanceTimeoutMs() {
    return rebalanceTimeoutMs;
  }

  /**
   * @return The client id.
   */
  public String clientId() {
    return clientId;
  }

  /**
   * @return The client host.
   */
  public String clientHost() {
    return clientHost;
  }

  /**
   * @return The list of subscribed topic names.
   */
  public List<String> subscribedTopicNames() {
    return subscribedTopicNames;
  }

  /**
   * @return The current state.
   */
  public MemberState state() {
    return state;
  }

  /**
   * @return The set of assigned partitions.
   */
  public Map<Uuid, Set<Integer>> assignedPartitions() {
    return assignedPartitions;
  }

  /**
   * @return A string representation of the current assignment state.
   */
  public String currentAssignmentSummary() {
    return "CurrentAssignment(memberEpoch=" + memberEpoch +
            ", previousMemberEpoch=" + previousMemberEpoch +
            ", targetMemberEpoch=" + targetMemberEpoch +
            ", state=" + state +
            ", assignedPartitions=" + assignedPartitions +
            ')';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShareGroupMember that = (ShareGroupMember) o;
    return memberEpoch == that.memberEpoch
            && previousMemberEpoch == that.previousMemberEpoch
            && targetMemberEpoch == that.targetMemberEpoch
            && rebalanceTimeoutMs == that.rebalanceTimeoutMs
            && Objects.equals(memberId, that.memberId)
            && Objects.equals(rackId, that.rackId)
            && Objects.equals(clientId, that.clientId)
            && Objects.equals(clientHost, that.clientHost)
            && Objects.equals(subscribedTopicNames, that.subscribedTopicNames)
            && Objects.equals(assignedPartitions, that.assignedPartitions);
  }

  @Override
  public int hashCode() {
    int result = memberId != null ? memberId.hashCode() : 0;
    result = 31 * result + memberEpoch;
    result = 31 * result + previousMemberEpoch;
    result = 31 * result + targetMemberEpoch;
    result = 31 * result + Objects.hashCode(rackId);
    result = 31 * result + rebalanceTimeoutMs;
    result = 31 * result + Objects.hashCode(clientId);
    result = 31 * result + Objects.hashCode(clientHost);
    result = 31 * result + Objects.hashCode(subscribedTopicNames);
    result = 31 * result + Objects.hashCode(assignedPartitions);
    return result;
  }

  @Override
  public String toString() {
    return "ConsumerGroupMember(" +
            "memberId='" + memberId + '\'' +
            ", memberEpoch=" + memberEpoch +
            ", previousMemberEpoch=" + previousMemberEpoch +
            ", targetMemberEpoch=" + targetMemberEpoch +
            ", rackId='" + rackId + '\'' +
            ", rebalanceTimeoutMs=" + rebalanceTimeoutMs +
            ", clientId='" + clientId + '\'' +
            ", clientHost='" + clientHost + '\'' +
            ", subscribedTopicNames=" + subscribedTopicNames +
            ", state=" + state +
            ", assignedPartitions=" + assignedPartitions +
            ')';
  }
}
