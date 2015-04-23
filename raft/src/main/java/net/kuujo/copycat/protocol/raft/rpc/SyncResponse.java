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
package net.kuujo.copycat.protocol.raft.rpc;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.SerializationException;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.protocol.raft.RaftError;
import net.kuujo.copycat.protocol.raft.RaftMember;
import net.kuujo.copycat.protocol.raft.RaftMemberPool;

import java.util.Collection;
import java.util.Objects;

/**
 * Protocol sync response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SyncResponse extends AbstractResponse<SyncResponse> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };
  private static final ThreadLocal<RaftMemberPool> memberPool = new ThreadLocal<RaftMemberPool>() {
    @Override
    protected RaftMemberPool initialValue() {
      return new RaftMemberPool();
    }
  };

  /**
   * Returns a new sync response builder.
   *
   * @return A new sync response builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a sync response builder for an existing response.
   *
   * @param response The response to build.
   * @return The sync response builder.
   */
  public static Builder builder(SyncResponse response) {
    return builder.get().reset(response);
  }

  private Collection<RaftMember> members;

  public SyncResponse(ReferenceManager<SyncResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.SYNC;
  }

  /**
   * Returns the responding node's membership.
   *
   * @return The responding node's membership.
   */
  public Collection<RaftMember> members() {
    return members;
  }

  @Override
  public void readObject(Buffer buffer) {
    status = Response.Status.forId(buffer.readByte());
    if (status == Response.Status.OK) {
      error = null;
      int membersSize = buffer.readInt();
      if (membersSize < 0)
        throw new SerializationException("invalid members size: " + membersSize);

      RaftMemberPool pool = memberPool.get();

      members.clear();
      for (int i = 0; i < membersSize; i++) {
        RaftMember member = pool.acquire();
        member.readObject(buffer);
        members.add(member);
      }
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(Buffer buffer) {
    buffer.writeByte(status.id());
    if (status == Response.Status.OK) {
      buffer.writeInt(members.size());
      for (RaftMember member : members) {
        member.writeObject(buffer);
      }
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public void close() {
    members.forEach(RaftMember::release);
    super.close();
  }

  @Override
  public int hashCode() {
    return Objects.hash(members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SyncResponse) {
      SyncResponse response = (SyncResponse) object;
      return response.status == status
        && response.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getSimpleName(), members);
  }

  /**
   * Sync response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, SyncResponse> {

    private Builder() {
      super(SyncResponse::new);
    }

    /**
     * Sets the response membership.
     *
     * @param members The request membership.
     * @return The sync response builder.
     */
    public Builder withMembers(Collection<RaftMember> members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      response.members = members;
      return this;
    }

    @Override
    public SyncResponse build() {
      super.build();
      if (response.members == null)
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
