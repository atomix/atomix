// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.cluster.RaftMember;

/**
 * Server join configuration change request.
 * <p>
 * The join request is the mechanism by which new servers join a cluster. When a server wants to
 * join a cluster, it must submit a join request to the leader. The leader will attempt to commit
 * the configuration change and, if successful, respond to the join request with the updated configuration.
 */
public class JoinRequest extends ConfigurationRequest {

  /**
   * Returns a new join request builder.
   *
   * @return A new join request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public JoinRequest(RaftMember member) {
    super(member);
  }

  /**
   * Join request builder.
   */
  public static class Builder extends ConfigurationRequest.Builder<Builder, JoinRequest> {
    @Override
    public JoinRequest build() {
      validate();
      return new JoinRequest(member);
    }
  }
}
