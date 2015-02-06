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
package net.kuujo.copycat.protocol.rpc;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Protocol query response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueryResponse extends AbstractResponse {

  /**
   * Returns a new query response builder.
   *
   * @return A new query response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a query response builder for an existing response.
   *
   * @param response The response to build.
   * @return The query response builder.
   */
  public static Builder builder(QueryResponse response) {
    return new Builder(response);
  }

  private ByteBuffer result;

  /**
   * Returns the query result.
   *
   * @return The query result.
   */
  public ByteBuffer result() {
    return result;
  }

  @Override
  public int hashCode() {
    return Objects.hash(member, status, result);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof QueryResponse) {
      QueryResponse response = (QueryResponse) object;
      return response.member.equals(member)
        && response.status == status
        && ((response.result == null && result == null)
        || response.result != null && result != null && response.result.equals(result));
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, result=%s]", getClass().getSimpleName(), status, result);
  }

  /**
   * Sync response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, QueryResponse> {
    private Builder() {
      this(new QueryResponse());
    }

    private Builder(QueryResponse response) {
      super(response);
    }

    /**
     * Sets the query response result.
     *
     * @param result The response result.
     * @return The response builder.
     */
    public Builder withResult(ByteBuffer result) {
      response.result = result;
      return this;
    }

    @Override
    public int hashCode() {
      return Objects.hash(response);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).response.equals(response);
    }

    @Override
    public String toString() {
      return String.format("%s[response=%s]", getClass().getCanonicalName(), response);
    }

  }

}
