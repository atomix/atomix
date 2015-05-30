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
package net.kuujo.copycat.raft.rpc;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.raft.Query;

import java.util.Objects;

/**
 * Protocol query request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueryRequest extends AbstractRequest<QueryRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new query request builder.
   *
   * @return A new query request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a query request builder for an existing request.
   *
   * @param request The request to build.
   * @return The query request builder.
   */
  public static Builder builder(QueryRequest request) {
    return builder.get().reset(request);
  }

  private long session;
  private Query query;

  public QueryRequest(ReferenceManager<QueryRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.QUERY;
  }

  /**
   * Returns the session ID.
   *
   * @return The session ID.
   */
  public long session() {
    return session;
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
  public void readObject(Buffer buffer, Serializer serializer) {
    query = serializer.readObject(buffer);
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    serializer.writeObject(query, buffer);
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
    return String.format("%s[session=%d, query=%s]", getClass().getSimpleName(), session, query);
  }

  /**
   * Query request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, QueryRequest> {

    protected Builder() {
      super(QueryRequest::new);
    }

    @Override
    Builder reset() {
      super.reset();
      request.session = 0;
      request.query = null;
      return this;
    }

    /**
     * Sets the session ID.
     *
     * @param session The session ID.
     * @return The request builder.
     */
    public Builder withSession(long session) {
      if (session <= 0)
        throw new IllegalArgumentException("session must be positive");
      request.session = session;
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
      if (request.session <= 0)
        throw new IllegalArgumentException("session must be positive");
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
