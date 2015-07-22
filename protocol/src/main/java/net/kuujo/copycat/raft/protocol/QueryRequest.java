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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.alleycat.util.ReferenceManager;
import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.raft.Query;

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

  private long version;
  private Query query;

  public QueryRequest(ReferenceManager<QueryRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.QUERY;
  }

  /**
   * Returns the request version.
   *
   * @return The request version.
   */
  public long version() {
    return version;
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
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    super.readObject(buffer, alleycat);
    version = buffer.readLong();
    query = alleycat.readObject(buffer);
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    super.writeObject(buffer, alleycat);
    buffer.writeLong(version);
    alleycat.writeObject(query, buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(query);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof QueryRequest && ((QueryRequest) object).query.equals(query);
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, version=%d, query=%s]", getClass().getSimpleName(), session, version, query);
  }

  /**
   * Query request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, QueryRequest> {

    private Builder(BuilderPool<Builder, QueryRequest> pool) {
      super(pool, QueryRequest::new);
    }

    @Override
    protected void reset() {
      super.reset();
      request.version = 0;
      request.query = null;
    }

    /**
     * Sets the request version.
     *
     * @param version The request version.
     * @return The request builder.
     */
    public Builder withVersion(long version) {
      if (version < 0)
        throw new IllegalArgumentException("version must be positive");
      request.version = version;
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
      if (request.version < 0)
        throw new IllegalArgumentException("version must be positive");
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
