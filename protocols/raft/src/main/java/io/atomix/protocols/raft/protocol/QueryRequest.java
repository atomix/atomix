/*
 * Copyright 2015-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftQuery;
import io.atomix.utils.ArraySizeHashPrinter;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client query request.
 * <p>
 * Query requests are submitted by clients to the Copycat cluster to commit {@link RaftQuery}s to
 * the replicated state machine. Each query request must be associated with a registered
 * {@link #session()} and have a unique {@link #sequence()} number within that session. Queries will
 * be applied in the cluster in the order defined by the provided sequence number. Thus, sequence numbers
 * should never be skipped. In the event of a failure of a query request, the request should be resent
 * with the same sequence number. Queries are guaranteed to be applied in sequence order.
 * <p>
 * Query requests should always be submitted to the server to which the client is connected. The provided
 * query's {@link RaftQuery#consistency() consistency level} will be used to determine how the query should be
 * handled. If the query is received by a follower, it may be evaluated on that node if the consistency level
 * is {@link RaftQuery.ConsistencyLevel#SEQUENTIAL}, otherwise it will be forwarded to the cluster leader.
 * Queries are always guaranteed to see state progress monotonically within a single {@link #session()}
 * even when switching servers.
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
  private final RaftQuery.ConsistencyLevel consistency;

  public QueryRequest(long session, long sequence, byte[] bytes, long index, RaftQuery.ConsistencyLevel consistency) {
    super(session, sequence, bytes);
    this.index = index;
    this.consistency = consistency;
  }

  /**
   * Returns the query index.
   *
   * @return The query index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the query consistency level.
   *
   * @return The query consistency level.
   */
  public RaftQuery.ConsistencyLevel consistency() {
    return consistency;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, sequence, index, bytes);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof QueryRequest) {
      QueryRequest request = (QueryRequest) object;
      return request.session == session
          && request.sequence == sequence
          && request.consistency == consistency
          && Arrays.equals(request.bytes, bytes);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", session)
        .add("sequence", sequence)
        .add("index", index)
        .add("consistency", consistency)
        .add("bytes", ArraySizeHashPrinter.of(bytes))
        .toString();
  }

  /**
   * Query request builder.
   */
  public static class Builder extends OperationRequest.Builder<Builder, QueryRequest> {
    private long index;
    private RaftQuery.ConsistencyLevel consistency = RaftQuery.ConsistencyLevel.LINEARIZABLE;

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

    /**
     * Sets the query consistency level.
     *
     * @param consistency The query consistency level.
     * @return The request builder.
     */
    public Builder withConsistency(RaftQuery.ConsistencyLevel consistency) {
      this.consistency = checkNotNull(consistency, "consistency cannot be null");
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkArgument(index >= 0, "index must be positive");
      checkNotNull(consistency, "consistency cannot be null");
    }

    /**
     * @throws IllegalStateException if {@code query} is null
     */
    @Override
    public QueryRequest build() {
      validate();
      return new QueryRequest(session, sequence, bytes, index, consistency);
    }
  }

}
