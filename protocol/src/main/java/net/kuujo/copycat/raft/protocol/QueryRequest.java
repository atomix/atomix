/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft.protocol;

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.Query;
import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol query request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=270)
public class QueryRequest extends SessionRequest<QueryRequest> {
  private static final BuilderPool<Builder, QueryRequest> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new query request builder.
   *
   * @return A new query request builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a query request builder for an existing request.
   *
   * @param request The request to build.
   * @return The query request builder.
   */
  public static Builder builder(QueryRequest request) {
    return POOL.acquire(request);
  }

  private long commandSequence;
  private Query query;

  public QueryRequest(ReferenceManager<QueryRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.QUERY;
  }

  /**
   * Returns the command sequence number.
   *
   * @return The command sequence number.
   */
  public long commandSequence() {
    return commandSequence;
  }

  /**
   * Returns the query.
   *
   * @return The query.
   */
  public Query query() {
    return query;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    commandSequence = buffer.readLong();
    query = serializer.readObject(buffer);
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(commandSequence);
    serializer.writeObject(query, buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, commandSequence, query);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof QueryRequest) {
      QueryRequest request = (QueryRequest) object;
      return request.session == session
        && request.commandSequence == commandSequence
        && request.query.equals(query);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, commandSequence=%d, query=%s]", getClass().getSimpleName(), session, commandSequence, query);
  }

  /**
   * Query request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, QueryRequest> {

    protected Builder(BuilderPool<Builder, QueryRequest> pool) {
      super(pool, QueryRequest::new);
    }

    @Override
    protected void reset() {
      super.reset();
      request.commandSequence = 0;
      request.query = null;
    }

    /**
     * Sets the command sequence number.
     *
     * @param commandSequence The command sequence number.
     * @return The request builder.
     */
    public Builder withCommandSequence(long commandSequence) {
      if (commandSequence < 0)
        throw new IllegalArgumentException("commandSequence cannot be less than 1");
      request.commandSequence = commandSequence;
      return this;
    }

    /**
     * Sets the request query.
     *
     * @param query The request query.
     * @return The request builder.
     */
    public Builder withQuery(Query query) {
      if (query == null)
        throw new NullPointerException("query cannot be null");
      request.query = query;
      return this;
    }

    @Override
    public QueryRequest build() {
      super.build();
      if (request.commandSequence < 0)
        throw new IllegalArgumentException("commandSequence cannot be less than 1");
      if (request.query == null)
        throw new NullPointerException("query cannot be null");
      return request;
    }

    @Override
    public int hashCode() {
      return Objects.hash(request);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).request.equals(request);
    }

    @Override
    public String toString() {
      return String.format("%s[request=%s]", getClass().getCanonicalName(), request);
    }

  }

}
