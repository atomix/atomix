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
package net.kuujo.copycat.raft.state;

import java.util.*;

/**
 * Cluster state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ClusterState implements Iterable<MemberState> {
  private final SortedMap<Integer, MemberState> members = new TreeMap<>();

  /**
   * Adds a member to the cluster state.
   *
   * @param member The member to add.
   * @return The cluster state.
   */
  ClusterState addMember(MemberState member) {
    members.put(member.getId(), member);
    return this;
  }

  /**
   * Removes a member from the cluster state.
   *
   * @param member The member to remove.
   * @return The cluster state.
   */
  ClusterState removeMember(MemberState member) {
    members.remove(member.getId());
    return this;
  }

  /**
   * Returns the member state for the given member.
   *
   * @param memberId The member ID.
   * @return The member state.
   */
  MemberState getMember(int memberId) {
    return members.get(memberId);
  }

  /**
   * Returns an ordered list of members.
   *
   * @return An ordered list of members.
   */
  List<MemberState> getMembers() {
    return new ArrayList<>(members.values());
  }

  @Override
  public Iterator<MemberState> iterator() {
    return new ClusterStateIterator(members.entrySet().iterator());
  }

  /**
   * Cluster state iterator.
   */
  private static class ClusterStateIterator implements Iterator<MemberState> {
    private final Iterator<Map.Entry<Integer, MemberState>> iterator;

    private ClusterStateIterator(Iterator<Map.Entry<Integer, MemberState>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public MemberState next() {
      return iterator.next().getValue();
    }

    @Override
    public void remove() {
      iterator.remove();
    }
  }

}
