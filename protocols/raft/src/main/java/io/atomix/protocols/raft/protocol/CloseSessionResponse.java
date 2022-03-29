// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;

import java.util.Objects;

/**
 * Close session response.
 */
public class CloseSessionResponse extends SessionResponse {

  /**
   * Returns a new keep alive response builder.
   *
   * @return A new keep alive response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public CloseSessionResponse(Status status, RaftError error) {
    super(status, error);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof CloseSessionResponse) {
      CloseSessionResponse response = (CloseSessionResponse) object;
      return response.status == status && Objects.equals(response.error, error);
    }
    return false;
  }

  /**
   * Status response builder.
   */
  public static class Builder extends SessionResponse.Builder<Builder, CloseSessionResponse> {
    @Override
    public CloseSessionResponse build() {
      validate();
      return new CloseSessionResponse(status, error);
    }
  }
}
