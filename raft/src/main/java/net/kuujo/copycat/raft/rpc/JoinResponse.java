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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.Buffer;
import net.kuujo.alleycat.util.ReferenceManager;
import net.kuujo.copycat.raft.RaftError;

import java.util.Objects;

/**
 * Protocol join response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=263)
public class JoinResponse extends AbstractResponse<JoinResponse> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new join response builder.
   *
   * @return A new join response builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a join response builder for an existing response.
   *
   * @param response The response to build.
   * @return The join response builder.
   */
  public static Builder builder(JoinResponse response) {
    return builder.get().reset(response);
  }

  private long term;
  private int leader;

  public JoinResponse(ReferenceManager<JoinResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.JOIN;
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
  public void readObject(Buffer buffer, Alleycat alleycat) {
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
  public void writeObject(Buffer buffer, Alleycat alleycat) {
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
    if (object instanceof JoinResponse) {
      JoinResponse response = (JoinResponse) object;
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
   * Join response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, JoinResponse> {

    private Builder() {
      super(JoinResponse::new);
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
     * @return The join response builder.
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
     * @return The join response builder.
     */
    public Builder withLeader(int leader) {
      response.leader = leader;
      return this;
    }

    @Override
    public JoinResponse build() {
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
