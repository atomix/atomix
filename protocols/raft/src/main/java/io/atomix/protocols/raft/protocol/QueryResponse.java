// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;

/**
 * Client query response.
 * <p>
 * Query responses are sent by servers to clients upon the completion of a
 * {@link QueryRequest}. Query responses are sent with the
 * {@link #index()} of the state machine at the point at which the query was evaluated.
 * This can be used by the client to ensure it sees state progress monotonically. Note, however, that
 * query responses may not be sent or received in sequential order. If a query response is proxied through
 * another server, responses may be received out of order. Clients should resequence concurrent responses
 * to ensure they're handled in FIFO order.
 */
public class QueryResponse extends OperationResponse {

  /**
   * Returns a new query response builder.
   *
   * @return A new query response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public QueryResponse(Status status, RaftError error, long index, long eventIndex, byte[] result, long lastSequence) {
    super(status, error, index, eventIndex, result, lastSequence);
  }

  /**
   * Query response builder.
   */
  public static class Builder extends OperationResponse.Builder<Builder, QueryResponse> {
    @Override
    public QueryResponse build() {
      validate();
      return new QueryResponse(status, error, index, eventIndex, result, lastSequence);
    }
  }
}
