// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.cluster.RaftMember;

import java.util.Collection;

/**
 * Server leave configuration change response.
 */
public class LeaveResponse extends ConfigurationResponse {

  /**
   * Returns a new leave response builder.
   *
   * @return A new leave response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public LeaveResponse(Status status, RaftError error, long index, long term, long timestamp, Collection<RaftMember> members) {
    super(status, error, index, term, timestamp, members);
  }

  /**
   * Leave response builder.
   */
  public static class Builder extends ConfigurationResponse.Builder<Builder, LeaveResponse> {
    @Override
    public LeaveResponse build() {
      validate();
      return new LeaveResponse(status, error, index, term, timestamp, members);
    }
  }
}
