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

import java.util.Objects;

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.raft.protocol.error.RaftError;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.util.ReferenceManager;

/**
 * Protocol register client response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=273)
public class RegisterResponse extends AbstractResponse<RegisterResponse> {

  /**
   * The unique identifier for the register response type.
   */
  public static final byte TYPE = 0x08;

  private static final BuilderPool<Builder, RegisterResponse> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new register client response builder.
   *
   * @return A new register client response builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a register client response builder for an existing response.
   *
   * @param response The response to build.
   * @return The register client response builder.
   * @throws NullPointerException if {@code response} is null
   */
  public static Builder builder(RegisterResponse response) {
    return POOL.acquire(Assert.notNull(response, "response"));
  }

  private long session;
  private Members members;

  /**
   * @throws NullPointerException if {@code referenceManager} is null
   */
  public RegisterResponse(ReferenceManager<RegisterResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public byte type() {
    return TYPE;
  }

  /**
   * Returns the registered session ID.
   *
   * @return The registered session ID.
   */
  public long session() {
    return session;
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
      session = buffer.readLong();
      members = serializer.readObject(buffer);
    } else {
      error = RaftError.forId(buffer.readByte());
      session = 0;
      members = null;
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.writeLong(session);
      serializer.writeObject(members, buffer);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, session, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof RegisterResponse) {
      RegisterResponse response = (RegisterResponse) object;
      return response.status == status
        && response.session == session
        && ((response.members == null && members == null)
        || (response.members != null && members != null && response.members.equals(members)));
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, session=%d, members=%s]", getClass().getSimpleName(), status, session, members);
  }

  /**
   * Register response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, RegisterResponse> {

    protected Builder(BuilderPool<Builder, RegisterResponse> pool) {
      super(pool, RegisterResponse::new);
    }

    @Override
    protected void reset() {
      super.reset();
      response.session = 0;
      response.members = null;
    }

    /**
     * Sets the response session ID.
     *
     * @param session The session ID.
     * @return The register response builder.
     * @throws IllegamArgumentException if {@code session} is less than 1
     */
    public Builder withSession(long session) {
      response.session = Assert.argNot(session, session < 1, "session cannot be less than 1");
      return this;
    }

    /**
     * Sets the response members.
     *
     * @param members The response members.
     * @return The response builder.
     * @throws NullPointerException if {@code members} is null
     */
    public Builder withMembers(Members members) {
      response.members = Assert.notNull(members, "members");
      return this;
    }

    /**
     * @throws IllegalStateException if status is OK and members is null
     */
    @Override
    public RegisterResponse build() {
      super.build();
      Assert.stateNot(response.status == Status.OK && response.members == null, "members cannot be null");
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
