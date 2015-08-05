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
import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol keep alive request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=262)
public class KeepAliveRequest extends SessionRequest<KeepAliveRequest> {

  /**
   * The unique identifier for the keep alive request type.
   */
  public static final byte TYPE = 0x09;

  private static final BuilderPool<Builder, KeepAliveRequest> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new keep alive request builder.
   *
   * @return A new keep alive request builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a keep alive request builder for an existing request.
   *
   * @param request The request to build.
   * @return The keep alive request builder.
   */
  public static Builder builder(KeepAliveRequest request) {
    return POOL.acquire(request);
  }

  private long commandSequence;

  public KeepAliveRequest(ReferenceManager<KeepAliveRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public byte type() {
    return TYPE;
  }

  /**
   * Returns the command sequence number.
   *
   * @return The command sequence number.
   */
  public long commandSequence() {
    return commandSequence;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    commandSequence = buffer.readLong();
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(commandSequence);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, commandSequence);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof KeepAliveRequest) {
      KeepAliveRequest request = (KeepAliveRequest) object;
      return request.session == session
        && request.commandSequence == commandSequence;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, commandSequence=%d]", getClass().getSimpleName(), session, commandSequence);
  }

  /**
   * Keep alive request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, KeepAliveRequest> {

    protected Builder(BuilderPool<Builder, KeepAliveRequest> pool) {
      super(pool, KeepAliveRequest::new);
    }

    @Override
    protected void reset() {
      super.reset();
      request.commandSequence = 0;
    }

    /**
     * Sets the command sequence number.
     *
     * @param commandSequence The command sequence number.
     * @return The request builder.
     */
    public Builder withCommandSequence(long commandSequence) {
      if (commandSequence < 0)
        throw new IllegalArgumentException("commandSequence cannot be negative");
      request.commandSequence = commandSequence;
      return this;
    }

    @Override
    public KeepAliveRequest build() {
      super.build();
      if (request.session <= 0)
        throw new IllegalArgumentException("session must be positive");
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
