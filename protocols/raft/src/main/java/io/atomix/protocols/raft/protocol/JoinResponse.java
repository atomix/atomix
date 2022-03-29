// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.RaftError;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Server join configuration change response.
 */
public class JoinResponse extends ConfigurationResponse {

  /**
   * Returns a new join response builder.
   *
   * @return A new join response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public JoinResponse(Status status, RaftError error, long index, long term, long timestamp, Collection<RaftMember> members) {
    super(status, error, index, term, timestamp, members);
  }

  /**
   * Join response builder.
   */
  public static class Builder extends ConfigurationResponse.Builder<Builder, JoinResponse> {
    @Override
    protected void validate() {
      // JoinResponse allows null errors indicating the client should retry.
      checkNotNull(status, "status cannot be null");
      if (status == Status.OK) {
        checkArgument(index >= 0, "index must be positive");
        checkArgument(term >= 0, "term must be positive");
        checkArgument(timestamp > 0, "time must be positive");
        checkNotNull(members, "members cannot be null");
      }
    }

    @Override
    public JoinResponse build() {
      validate();
      return new JoinResponse(status, error, index, term, timestamp, members);
    }
  }
}
