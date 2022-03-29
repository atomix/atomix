// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.cluster.RaftMember;

/**
 * Server leave configuration request.
 * <p>
 * The leave request is the mechanism by which servers remove themselves from a cluster. When a server
 * wants to leave a cluster, it must submit a leave request to the leader. The leader will attempt to commit
 * the configuration change and, if successful, respond to the join request with the updated configuration.
 */
public class LeaveRequest extends ConfigurationRequest {

  /**
   * Returns a new leave request builder.
   *
   * @return A new leave request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public LeaveRequest(RaftMember member) {
    super(member);
  }

  /**
   * Leave request builder.
   */
  public static class Builder extends ConfigurationRequest.Builder<Builder, LeaveRequest> {
    @Override
    public LeaveRequest build() {
      validate();
      return new LeaveRequest(member);
    }
  }
}
