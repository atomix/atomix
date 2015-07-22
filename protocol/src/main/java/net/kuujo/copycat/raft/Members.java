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

import java.util.*;

/**
 * Members.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Members implements AlleycatSerializable {

  /**
   * Returns a new members builder.
   *
   * @return A new members builder.
   */
  public static Builder builder() {
    return new Builder();
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

  /**
   * Configures the members.
   *
   * @param members The members with which to configure the members.
   */
  public void configure(Members members) {
    Iterator<Map.Entry<Integer, Member>> iterator = this.members.entrySet().iterator();
    while (iterator.hasNext()) {
      Member.Type type = iterator.next().getValue().type();
      if (type == Member.Type.PASSIVE || type == Member.Type.CLIENT) {
        iterator.remove();
      }
    }

    for (Member member : members.members()) {
      if (member.type() == Member.Type.ACTIVE || member.type() == Member.Type.CLIENT) {
        this.members.put(member.id(), member);
      }
    }
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

    private Builder() {
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
     * Adds a cluster seed member.
     *
     * @param member The cluster seed member to add.
     * @return The cluster builder.
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

    @Override
    public Members build() {
      return members;
    }
  }

}
