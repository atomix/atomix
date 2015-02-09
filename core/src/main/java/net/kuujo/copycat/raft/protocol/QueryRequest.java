/*
 * Copyright 2014 the original author or authors.
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

import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.util.internal.Assert;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Protocol query request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueryRequest extends AbstractRequest {

  /**
   * Returns a new query request builder.
   *
   * @return A new query request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a query request builder for an existing request.
   *
   * @param request The request to build.
   * @return The query request builder.
   */
  public static Builder builder(QueryRequest request) {
    return new Builder(request);
  }

  private ByteBuffer entry;
  private Consistency consistency = Consistency.DEFAULT;

  /**
   * Returns the query entry.
   *
   * @return The query entry.
   */
  public ByteBuffer entry() {
    return entry;
  }

  /**
   * Returns the query consistency level.
   *
   * @return The query consistency level.
   */
  public Consistency consistency() {
    return consistency;
  }

  @Override
  public int hashCode() {
    return Objects.hash(member, entry, consistency);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof QueryRequest) {
      QueryRequest request = (QueryRequest) object;
      return request.member.equals(member)
        && request.entry.equals(entry)
        && request.consistency == consistency;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[entry=%s, consistency=%s]", getClass().getSimpleName(), entry.toString(), consistency);
  }

  /**
   * Sync request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, QueryRequest> {
    private Builder() {
      this(new QueryRequest());
    }

    private Builder(QueryRequest request) {
      super(request);
    }

    /**
     * Sets the request entry.
     *
     * @param entry The request entry.
     * @return The request builder.
     */
    public Builder withEntry(ByteBuffer entry) {
      request.entry = Assert.isNotNull(entry, "entry");
      return this;
    }

    /**
     * Sets the request consistency level.
     *
     * @param consistency The request consistency level.
     * @return The request builder.
     */
    public Builder withConsistency(Consistency consistency) {
      request.consistency = Assert.isNotNull(consistency, "consistency");
      return this;
    }

    @Override
    public QueryRequest build() {
      super.build();
      Assert.isNotNull(request.entry, "entry");
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
