// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;

/**
 * Leadership transfer response.
 */
public class TransferResponse extends AbstractRaftResponse {

  /**
   * Returns a new transfer response builder.
   *
   * @return A new transfer response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public TransferResponse(Status status, RaftError error) {
    super(status, error);
  }

  /**
   * Join response builder.
   */
  public static class Builder extends AbstractRaftResponse.Builder<Builder, TransferResponse> {
    @Override
    public TransferResponse build() {
      validate();
      return new TransferResponse(status, error);
    }
  }
}
