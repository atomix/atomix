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
import net.kuujo.copycat.raft.RaftError;

import java.util.Objects;

/**
 * Protocol query response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueryResponse extends AbstractResponse<QueryResponse> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new query response builder.
   *
   * @return A new query response builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a query response builder for an existing request.
   *
   * @param request The response to build.
   * @return The query response builder.
   */
  public static Builder builder(QueryResponse request) {
    return builder.get().reset(request);
  }

  private Object result;

  public QueryResponse(ReferenceManager<QueryResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.QUERY;
  }

  /**
   * Returns the query result.
   *
   * @return The query result.
   */
  public Object result() {
    return result;
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      result = serializer.readObject(buffer);
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      serializer.writeObject(result, buffer);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, result);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof QueryResponse) {
      QueryResponse response = (QueryResponse) object;
      return response.status == status
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
   * Query response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, QueryResponse> {

    private Builder() {
      super(QueryResponse::new);
    }

    @Override
    Builder reset() {
      super.reset();
      response.result = null;
      return this;
    }

    /**
     * Sets the query response result.
     *
     * @param result The response result.
     * @return The response builder.
     */
    @SuppressWarnings("unchecked")
    public Builder withResult(Object result) {
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
