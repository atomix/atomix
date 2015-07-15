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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.alleycat.util.ReferenceManager;
import net.kuujo.copycat.raft.RaftError;

import java.util.Objects;

/**
 * Protocol heartbeat response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=261)
public class HeartbeatResponse extends AbstractResponse<HeartbeatResponse> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new heartbeat response builder.
   *
   * @return A new heartbeat response builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a heartbeat response builder for an existing response.
   *
   * @param response The response to build.
   * @return The heartbeat response builder.
   */
  public static Builder builder(HeartbeatResponse response) {
    return builder.get().reset(response);
  }

  private long term;
  private int leader;

  public HeartbeatResponse(ReferenceManager<HeartbeatResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.HEARTBEAT;
  }

  /**
   * Returns the responding node's current term.
   *
   * @return The responding node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the responding node's current leader.
   *
   * @return The responding node's current leader.
   */
  public int leader() {
    return leader;
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      term = buffer.readLong();
      leader = buffer.readInt();
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.writeLong(term).writeInt(leader);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, term, leader);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof HeartbeatResponse) {
      HeartbeatResponse response = (HeartbeatResponse) object;
      return response.status == status
        && response.term == term
        && response.leader == leader;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, leader=%d]", getClass().getSimpleName(), term, leader);
  }

  /**
   * Heartbeat response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, HeartbeatResponse> {

    private Builder() {
      super(HeartbeatResponse::new);
    }

    @Override
    Builder reset() {
      super.reset();
      response.term = 0;
      response.leader = 0;
      return this;
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The heartbeat response builder.
     */
    public Builder withTerm(long term) {
      if (term < 0)
        throw new IllegalArgumentException("term cannot be negative");
      response.term = term;
      return this;
    }

    /**
     * Sets the response leader.
     *
     * @param leader The response leader.
     * @return The heartbeat response builder.
     */
    public Builder withLeader(int leader) {
      response.leader = leader;
      return this;
    }

    @Override
    public HeartbeatResponse build() {
      super.build();
      if (response.term < 0)
        throw new IllegalArgumentException("term cannot be negative");
      if (response.leader < 0)
        throw new IllegalArgumentException("leader cannot be negative");
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
