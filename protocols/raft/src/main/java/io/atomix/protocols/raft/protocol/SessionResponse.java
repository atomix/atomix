// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;

/**
 * Base session response.
 */
public abstract class SessionResponse extends AbstractRaftResponse {
  protected SessionResponse(Status status, RaftError error) {
    super(status, error);
  }

  /**
   * Session response builder.
   */
  public abstract static class Builder<T extends Builder<T, U>, U extends SessionResponse> extends AbstractRaftResponse.Builder<T, U> {
  }
}
