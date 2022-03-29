// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;

/**
 * Client command response.
 * <p>
 * Command responses are sent by servers to clients upon the completion of a
 * {@link CommandRequest}. Command responses are sent with the
 * {@link #index()} (or index) of the state machine at the point at which the command was evaluated.
 * This can be used by the client to ensure it sees state progress monotonically. Note, however, that
 * command responses may not be sent or received in sequential order. If a command response has to await
 * the completion of an event, or if the response is proxied through another server, responses may be
 * received out of order. Clients should resequence concurrent responses to ensure they're handled in FIFO order.
 */
public class CommandResponse extends OperationResponse {

  /**
   * Returns a new submit response builder.
   *
   * @return A new submit response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public CommandResponse(Status status, RaftError error, long index, long eventIndex, byte[] result, long lastSequence) {
    super(status, error, index, eventIndex, result, lastSequence);
  }

  /**
   * Command response builder.
   */
  public static class Builder extends OperationResponse.Builder<Builder, CommandResponse> {
    @Override
    public CommandResponse build() {
      validate();
      return new CommandResponse(status, error, index, eventIndex, result, lastSequence);
    }
  }
}
