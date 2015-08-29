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

import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.CopycatSerializable;
import net.kuujo.copycat.io.serializer.Serializer;

import java.util.*;

/**
 * Container for Raft member configurations.
 * <p>
 * Members are immutable and therefore threadsafe. To create a {@link Members} instance, create a
 * {@link Members.Builder} via the static {@link Members#builder()} method.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Members implements CopycatSerializable {
  private static final BuilderPool<Builder, Members> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new members builder.
   * <p>
   * The returned {@link Members.Builder} is pooled internally via a {@link net.kuujo.copycat.util.BuilderPool}.
   * Once the builder's {@link Members.Builder#build()} method is called, the builder will be
   * released back to the internal pool and recycled on the next call to this method.
   *
   * @return A new members builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a new Builder for the {@code members}.
   * 
   * @throws NullPointerException if {@code members} is null
   */
  public static Builder builder(Members members) {
    return POOL.acquire(Assert.notNull(members, "members"));
  }

  private final Map<Integer, Member> members = new HashMap<>();
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
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    serializer.writeObject(list, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    list = serializer.readObject(buffer);
    for (Member member : list) {
      members.put(member.id(), member);
    }
  }

  @Override
  public String toString() {
    return String.format("%s%s", getClass().getSimpleName(), members.values());
  }

  /**
   * Members builder.
   */
  public static class Builder extends net.kuujo.copycat.util.Builder<Members> {
    private Members members = new Members();

    public Builder(BuilderPool<Builder, Members> pool) {
      super(pool);
    }

    @Override
    protected void reset() {
      super.reset();
      this.members = new Members();
    }

    @Override
    protected void reset(Members members) {
      this.members = Assert.notNull(members, "members");
    }

    /**
     * Sets the cluster seed members.
     *
     * @param members The set of cluster seed members.
     * @return The cluster builder.
     * @throws NullPointerException if {@code members} is null
     */
    public Builder withMembers(Member... members) {
      return withMembers(Arrays.asList(Assert.notNull(members, "members")));
    }

    /**
     * Sets the cluster seed members.
     *
     * @param members The set of cluster seed members.
     * @return The cluster builder.
     * @throws NullPointerException if {@code members} is null
     */
    public Builder withMembers(Collection<Member> members) {
      Assert.notNull(members, "members");
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
     * @throws NullPointerException if {@code member} is null
     */
    public Builder addMember(Member member) {
      Assert.notNull(member, "member");
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
     * @throws NullPointerException if {@code member} is null
     */
    public Builder removeMember(Member member) {
      Assert.notNull(member, "member");
      this.members.members.remove(member.id());
      this.members.list.remove(member);
      return this;
    }

    @Override
    public Members build() {
      close();
      return members;
    }
  }

}
