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
package net.kuujo.copycat.raft;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.copycat.BuilderPool;

import java.util.*;

/**
 * Members.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Members implements AlleycatSerializable {
  private static final BuilderPool<Builder, Members> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new members builder.
   *
   * @return A new members builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  public static Builder builder(Members members) {
    return POOL.acquire(members);
  }

  private Map<Integer, Member> members = new HashMap<>();
  private List<Member> list = new ArrayList<>();

  /**
   * Returns the collection of members.
   *
   * @return The collection of members.
   */
  public List<Member> members() {
    return list;
  }

  /**
   * Returns a member by ID.
   *
   * @param id The member ID.
   * @return The member or {@code null} if the member doesn't exist.
   */
  public Member member(int id) {
    return members.get(id);
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    alleycat.writeObject(members, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    members = alleycat.readObject(buffer);
  }

  /**
   * Members builder.
   */
  public static class Builder extends net.kuujo.copycat.Builder<Members> {
    private Members members = new Members();

    public Builder(BuilderPool<Builder, Members> pool) {
      super(pool);
    }

    @Override
    protected void reset() {
      members.members.clear();
      members.list.clear();
    }

    @Override
    protected void reset(Members members) {
      this.members = members;
    }

    /**
     * Sets the cluster seed members.
     *
     * @param members The set of cluster seed members.
     * @return The cluster builder.
     */
    @SuppressWarnings("unchecked")
    public Builder withMembers(Member... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return withMembers(Arrays.asList(members));
    }

    /**
     * Sets the cluster seed members.
     *
     * @param members The set of cluster seed members.
     * @return The cluster builder.
     */
    public Builder withMembers(Collection<Member> members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");

      this.members.members.clear();
      this.members.list.clear();
      for (Member member : members) {
        this.members.members.put(member.id(), member);
        this.members.list.add(member);
      }
      return this;
    }

    /**
     * Adds a member.
     *
     * @param member The member to add.
     * @return The members builder.
     */
    public Builder addMember(Member member) {
      if (member == null)
        throw new NullPointerException("member cannot be null");

      this.members.members.put(member.id(), member);
      if (!this.members.list.contains(member)) {
        this.members.list.add(member);
      }
      return this;
    }

    /**
     * Removes a member.
     *
     * @param member The member to remove.
     * @return The members builder.
     */
    public Builder removeMember(Member member) {
      if (member == null)
        throw new NullPointerException("member cannot be null");

      this.members.members.remove(member.id());
      this.members.list.remove(member);
      return this;
    }

    @Override
    public Members build() {
      return members;
    }
  }

}
