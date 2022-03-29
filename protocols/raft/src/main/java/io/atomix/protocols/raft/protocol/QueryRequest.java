// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.primitive.operation.PrimitiveOperation;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Client query request.
 * <p>
 * Query requests are submitted by clients to the Raft cluster to commit {@link PrimitiveOperation}s to
 * the replicated state machine. Each query request must be associated with a registered
 * {@link #session()} and have a unique {@link #sequenceNumber()} number within that session. Queries will
 * be applied in the cluster in the order defined by the provided sequence number. Thus, sequence numbers
 * should never be skipped. In the event of a failure of a query request, the request should be resent
 * with the same sequence number. Queries are guaranteed to be applied in sequence order.
 */
public class QueryRequest extends OperationRequest {

  /**
   * Returns a new query request builder.
   *
   * @return A new query request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final long index;

  public QueryRequest(long session, long sequence, PrimitiveOperation operation, long index) {
    super(session, sequence, operation);
    this.index = index;
  }

  /**
   * Returns the query index.
   *
   * @return The query index.
   */
  public long index() {
    return index;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, sequence, operation, index);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof QueryRequest) {
      QueryRequest request = (QueryRequest) object;
      return request.session == session
          && request.sequence == sequence
          && request.operation.equals(operation);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", session)
        .add("sequence", sequence)
        .add("operation", operation)
        .add("index", index)
        .toString();
  }

  /**
   * Query request builder.
   */
  public static class Builder extends OperationRequest.Builder<Builder, QueryRequest> {
    private long index;

    /**
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code index} is less than {@code 0}
     */
    public Builder withIndex(long index) {
      checkArgument(index >= 0, "index must be positive");
      this.index = index;
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkArgument(index >= 0, "index must be positive");
    }

    /**
     * @throws IllegalStateException if {@code query} is null
     */
    @Override
    public QueryRequest build() {
      validate();
      return new QueryRequest(session, sequence, operation, index);
    }
  }

}
