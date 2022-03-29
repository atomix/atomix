// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.cluster;

import io.atomix.utils.event.AbstractEvent;

/**
 * Raft cluster event.
 */
public class RaftClusterEvent extends AbstractEvent<RaftClusterEvent.Type, RaftMember> {

  /**
   * Raft cluster event type.
   */
  public enum Type {
    JOIN,
    LEAVE,
  }

  public RaftClusterEvent(Type type, RaftMember subject) {
    super(type, subject);
  }

  public RaftClusterEvent(Type type, RaftMember subject, long time) {
    super(type, subject, time);
  }
}
