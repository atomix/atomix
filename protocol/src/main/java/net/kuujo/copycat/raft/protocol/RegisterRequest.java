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

import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.Objects;
import java.util.UUID;

/**
 * Protocol register client request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=272)
public class RegisterRequest extends ClientRequest<RegisterRequest> {
  private static final BuilderPool<Builder, RegisterRequest> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new register client request builder.
   *
   * @return A new register client request builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a register client request builder for an existing request.
   *
   * @param request The request to build.
   * @return The register client request builder.
   */
  public static Builder builder(RegisterRequest request) {
    return POOL.acquire(request);
  }

  private UUID connection;

  public RegisterRequest(ReferenceManager<RegisterRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.REGISTER;
  }

  /**
   * Returns the connection ID.
   *
   * @return The connection ID.
   */
  public UUID connection() {
    return connection;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    serializer.writeObject(connection, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    connection = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s", getClass().getSimpleName());
  }

  /**
   * Register client request builder.
   */
  public static class Builder extends ClientRequest.Builder<Builder, RegisterRequest> {

    private Builder(BuilderPool<Builder, RegisterRequest> pool) {
      super(pool, RegisterRequest::new);
    }

    @Override
    protected void reset() {
      super.reset();
      request.connection = null;
    }

    /**
     * Sets the connection ID.
     *
     * @param connection The connection ID.
     * @return The request builder.
     */
    public Builder withConnection(UUID connection) {
      if (connection == null)
        throw new NullPointerException("connection cannot be null");
      request.connection = connection;
      return this;
    }

    @Override
    public RegisterRequest build() {
      super.build();
      if (request.connection == null)
        throw new NullPointerException("connection cannot be null");
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
