// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;

/**
 * Client heartbeat response.
 */
public class HeartbeatResponse extends AbstractRaftResponse {

  /**
   * Returns a new heartbeat response builder.
   *
   * @return A new heartbeat response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public HeartbeatResponse(Status status, RaftError error) {
    super(status, error);
  }

  /**
   * Heartbeat response builder.
   */
  public static class Builder extends AbstractRaftResponse.Builder<Builder, HeartbeatResponse> {
    @Override
    public HeartbeatResponse build() {
      validate();
      return new HeartbeatResponse(status, error);
    }
  }
}
