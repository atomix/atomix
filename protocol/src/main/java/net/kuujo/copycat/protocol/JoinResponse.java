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
package net.kuujo.copycat.protocol;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.alleycat.util.ReferenceManager;
import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.Members;
import net.kuujo.copycat.RaftError;

import java.util.Objects;

/**
 * Protocol join response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=261)
public class JoinResponse extends AbstractResponse<JoinResponse> {
  private static final BuilderPool<Builder, JoinResponse> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new join response builder.
   *
   * @return A new join response builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns an join response builder for an existing response.
   *
   * @param response The response to build.
   * @return The join response builder.
   */
  public static Builder builder(JoinResponse response) {
    return POOL.acquire(response);
  }

  private long version;
  private Members active;
  private Members passive;

  public JoinResponse(ReferenceManager<JoinResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.JOIN;
  }

  /**
   * Returns the response version.
   *
   * @return The response version.
   */
  public long version() {
    return version;
  }

  /**
   * Returns the join members list.
   *
   * @return The join members list.
   */
  public Members active() {
    return active;
  }

  /**
   * Returns the join members list.
   *
   * @return The join members list.
   */
  public Members passive() {
    return passive;
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      version = buffer.readLong();
      active = serializer.readObject(buffer);
      passive = serializer.readObject(buffer);
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat serializer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.writeLong(version);
      serializer.writeObject(active, buffer);
      serializer.writeObject(passive, buffer);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, active, passive);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof JoinResponse) {
      JoinResponse response = (JoinResponse) object;
      return response.status == status
        && response.version == version
        && response.active.equals(active)
        && response.passive.equals(passive);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, version=%d, active=%b, passive=%s]", getClass().getSimpleName(), status, version, active, passive);
  }

  /**
   * Join response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, JoinResponse> {

    private Builder(BuilderPool<Builder, JoinResponse> pool) {
      super(pool, JoinResponse::new);
    }

    @Override
    protected void reset() {
      super.reset();
      response.version = 0;
      response.active = null;
      response.passive = null;
    }

    /**
     * Sets the response version.
     *
     * @param version The response version.
     * @return The response builder.
     */
    public Builder withVersion(long version) {
      if (version < 0)
        throw new IllegalArgumentException("version cannot be negative");
      response.version = version;
      return this;
    }

    /**
     * Sets the response members.
     *
     * @param members The response members.
     * @return The response builder.
     */
    public Builder withActiveMembers(Members members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      response.active = members;
      return this;
    }

    /**
     * Sets the response members.
     *
     * @param members The response members.
     * @return The response builder.
     */
    public Builder withPassiveMembers(Members members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      response.passive = members;
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
    public JoinResponse build() {
      super.build();
      if (response.status == Status.OK) {
        if (response.active == null)
          throw new NullPointerException("active members cannot be null");
        if (response.passive == null)
          throw new NullPointerException("passive members cannot be null");
      }
      return response;
    }

    @Override
    public String toString() {
      return String.format("%s[response=%s]", getClass().getCanonicalName(), response);
    }

  }

}
