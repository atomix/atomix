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
import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.raft.RaftError;
import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol keep alive response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=263)
public class KeepAliveResponse extends SessionResponse<KeepAliveResponse> {
  private static final BuilderPool<Builder, KeepAliveResponse> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new keep alive response builder.
   *
   * @return A new keep alive response builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a keep alive response builder for an existing response.
   *
   * @param response The response to build.
   * @return The keep alive response builder.
   */
  public static Builder builder(KeepAliveResponse response) {
    return POOL.acquire(response);
  }

  private Members members;

  public KeepAliveResponse(ReferenceManager<KeepAliveResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.KEEP_ALIVE;
  }

  /**
   * Returns the cluster members.
   *
   * @return The cluster members.
   */
  public Members members() {
    return members;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      members = serializer.readObject(buffer);
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      serializer.writeObject(members, buffer);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof KeepAliveResponse) {
      KeepAliveResponse response = (KeepAliveResponse) object;
      return response.status == status;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s]", getClass().getSimpleName(), status);
  }

  /**
   * Status response builder.
   */
  public static class Builder extends SessionResponse.Builder<Builder, KeepAliveResponse> {

    protected Builder(BuilderPool<Builder, KeepAliveResponse> pool) {
      super(pool, KeepAliveResponse::new);
    }

    @Override
    protected void reset() {
      super.reset();
      response.members = null;
    }

    /**
     * Sets the response members.
     *
     * @param members The response members.
     * @return The response builder.
     */
    public Builder withMembers(Members members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      response.members = members;
      return this;
    }

    @Override
    public KeepAliveResponse build() {
      super.build();
      if (response.status == Status.OK && response.members == null)
        throw new NullPointerException("members cannot be null");
      return response;
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
