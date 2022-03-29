// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;

/**
 * Configuration installation response.
 */
public class ConfigureResponse extends AbstractRaftResponse {

  /**
   * Returns a new configure response builder.
   *
   * @return A new configure response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public ConfigureResponse(Status status, RaftError error) {
    super(status, error);
  }

  /**
   * Heartbeat response builder.
   */
  public static class Builder extends AbstractRaftResponse.Builder<Builder, ConfigureResponse> {
    @Override
    public ConfigureResponse build() {
      return new ConfigureResponse(status, error);
    }
  }
}
