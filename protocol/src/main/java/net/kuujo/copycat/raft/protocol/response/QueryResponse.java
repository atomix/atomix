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
package net.kuujo.copycat.raft.protocol.response;

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.protocol.error.RaftError;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol query response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=271)
public class QueryResponse extends SessionResponse<QueryResponse> {

  /**
   * The unique identifier for the query response type.
   */
  public static final byte TYPE = 0x04;

  private static final BuilderPool<Builder, QueryResponse> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new query response builder.
   *
   * @return A new query response builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a query response builder for an existing request.
   *
   * @param request The response to build.
   * @return The query response builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(QueryResponse request) {
    return POOL.acquire(Assert.notNull(request, "request"));
  }

  private long version;
  private Object result;

  /**
   * @throws NullPointerException if {@code referenceManager} is null
   */
  public QueryResponse(ReferenceManager<QueryResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public byte type() {
    return TYPE;
  }

  /**
   * Returns the query version.
   *
   * @return The query version.
   */
  public long version() {
    return version;
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
  public void readObject(BufferInput buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      version = buffer.readLong();
      result = serializer.readObject(buffer);
    } else {
      version = buffer.readLong();
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.writeLong(version);
      serializer.writeObject(result, buffer);
    } else {
      buffer.writeLong(version);
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, result);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof QueryResponse) {
      QueryResponse response = (QueryResponse) object;
      return response.status == status
        && response.version == version
        && ((response.result == null && result == null)
        || response.result != null && result != null && response.result.equals(result));
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, version=%d, result=%s]", getClass().getSimpleName(), status, version, result);
  }

  /**
   * Query response builder.
   */
  public static class Builder extends SessionResponse.Builder<Builder, QueryResponse> {

    protected Builder(BuilderPool<Builder, QueryResponse> pool) {
      super(pool, QueryResponse::new);
    }

    @Override
    protected void reset() {
      super.reset();
      response.version = 0;
      response.result = null;
    }

    /**
     * Sets the query version number.
     *
     * @param version The query version number.
     * @return The response builder
     * @throws IllegalArgumentException if {@code version} is less than 0
     */
    public Builder withVersion(long version) {
      response.version = Assert.argNot(version, version < 0, "version cannot be negative");
      return this;
    }

    /**
     * Sets the query response result.
     *
     * @param result The response result.
     * @return The response builder.
     * @throws NullPointerException if {@code result} is null
     */
    public Builder withResult(Object result) {
      response.result = Assert.notNull(result, "result");
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
