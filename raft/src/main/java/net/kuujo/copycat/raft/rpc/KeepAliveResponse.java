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

import net.kuujo.copycat.cluster.TypedMemberInfo;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.raft.RaftError;

import java.util.*;

/**
 * Protocol keep alive response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeepAliveResponse extends AbstractResponse<KeepAliveResponse> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new keep alive response builder.
   *
   * @return A new keep alive response builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a keep alive response builder for an existing response.
   *
   * @param response The response to build.
   * @return The keep alive response builder.
   */
  public static Builder builder(KeepAliveResponse response) {
    return builder.get().reset(response);
  }

  private long term;
  private int leader;
  private Set<TypedMemberInfo> members = new HashSet<>();

  public KeepAliveResponse(ReferenceManager<KeepAliveResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.KEEP_ALIVE;
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

  /**
   * Returns the responding node's member set.
   *
   * @return The responding node's member set.
   */
  public Collection<TypedMemberInfo> members() {
    return members;
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      term = buffer.readLong();
      leader = buffer.readInt();
      members = serializer.readObject(buffer);
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.writeLong(term).writeInt(leader);
      serializer.writeObject(members, buffer);
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
    if (object instanceof KeepAliveResponse) {
      KeepAliveResponse response = (KeepAliveResponse) object;
      return response.status == status
        && response.term == term
        && response.leader == leader
        && response.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, leader=%b, members=%s]", getClass().getSimpleName(), term, leader, members);
  }

  /**
   * Status response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, KeepAliveResponse> {

    private Builder() {
      super(KeepAliveResponse::new);
    }

    @Override
    Builder reset() {
      super.reset();
      response.term = 0;
      response.leader = 0;
      response.members.clear();
      return this;
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The keep alive response builder.
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
     * @return The keep alive response builder.
     */
    public Builder withLeader(int leader) {
      response.leader = leader;
      return this;
    }

    /**
     * Sets the response members.
     *
     * @param members The response members.
     * @return The keep alive response builder.;
     */
    public Builder withMembers(TypedMemberInfo... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return withMembers(Arrays.asList(members));
    }

    /**
     * Sets the response members.
     *
     * @param members The response members.
     * @return The keep alive response builder.;
     */
    public Builder withMembers(Collection<TypedMemberInfo> members) {
      response.members.addAll(members);
      return this;
    }

    @Override
    public KeepAliveResponse build() {
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
