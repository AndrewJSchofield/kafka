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

import java.util.Objects;

/**
 * The CurrentAssignmentBuilder class encapsulates the reconciliation engine of the
 * share group protocol. Given the current state of a member and a desired or target
 * assignment state, the state machine takes the necessary steps to converge them.
 *
 * The member state has the following properties:
 * - Current Epoch:
 *   The current epoch of the member.
 *
 * - Next Epoch:
 *   The desired epoch of the member. It corresponds to the epoch of the target/desired assignment.
 *   The member transitions to this epoch when it has revoked the partitions that it does not own
 *   or if it does not have to revoke any.
 *
 * - Previous Epoch:
 *   The epoch of the member when the state was last updated.
 *
 * - Assigned Partitions:
 *   The set of partitions currently assigned to the member. This represents what the member should have.
 *
 * The state machine has just one state:
 * - STABLE:
 *   This state means that the member has received all its assigned partitions.
 *
 * The reconciliation process is started or re-started whenever a new target assignment is installed;
 * the epoch of the new target assignment is different from the next epoch of the member.
 */
public class CurrentAssignmentBuilder {
  /**
   * The share group member which is reconciled.
   */
  private final ShareGroupMember member;

  /**
   * The target assignment epoch.
   */
  private int targetAssignmentEpoch;

  /**
   * The target assignment.
   */
  private Assignment targetAssignment;

  /**
   * Constructs the CurrentAssignmentBuilder based on the current state of the
   * provided share group member.
   *
   * @param member The share group member that must be reconciled.
   */
  public CurrentAssignmentBuilder(ShareGroupMember member) {
    this.member = Objects.requireNonNull(member);
  }

  /**
   * Sets the target assignment epoch and the target assignment that the
   * share group member must be reconciled to.
   *
   * @param targetAssignmentEpoch The target assignment epoch.
   * @param targetAssignment      The target assignment.
   * @return This object.
   */
  public CurrentAssignmentBuilder withTargetAssignment(
          int targetAssignmentEpoch,
          Assignment targetAssignment
  ) {
    this.targetAssignmentEpoch = targetAssignmentEpoch;
    this.targetAssignment = Objects.requireNonNull(targetAssignment);
    return this;
  }

  /**
   * Builds the next state for the member or keep the current one if it
   * is not possible to move forward with the current state.
   *
   * @return A new ShareGroupMember or the current one.
   */
  public ShareGroupMember build() {
    // A new target assignment has been installed, we need to restart
    // the reconciliation loop from the beginning.
    if (targetAssignmentEpoch != member.targetMemberEpoch()) {
      return transitionToNewTargetAssignmentState();
    }

    return member;
  }

  /**
   * Transitions to NewTargetAssignment state.
   *
   * @return A new ShareGroupMember.
   */
  private ShareGroupMember transitionToNewTargetAssignmentState() {
    // We transition to the target epoch. The transition to the new state is done
    // when the member is updated.
    return new ShareGroupMember.Builder(member)
            .setAssignedPartitions(targetAssignment.partitions())
            .setMemberEpoch(targetAssignmentEpoch)
            .setTargetMemberEpoch(targetAssignmentEpoch)
            .build();
  }
}
