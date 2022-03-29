// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.cluster.RaftMember;

import java.util.Collection;

/**
 * Server configuration change response.
 */
public class ReconfigureResponse extends ConfigurationResponse {

  /**
   * Returns a new reconfigure response builder.
   *
   * @return A new reconfigure response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public ReconfigureResponse(Status status, RaftError error, long index, long term, long timestamp, Collection<RaftMember> members) {
    super(status, error, index, term, timestamp, members);
  }

  /**
   * Reconfigure response builder.
   */
  public static class Builder extends ConfigurationResponse.Builder<Builder, ReconfigureResponse> {
    @Override
    public ReconfigureResponse build() {
      validate();
      return new ReconfigureResponse(status, error, index, term, timestamp, members);
    }
  }
}
