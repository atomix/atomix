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

import net.kuujo.copycat.cluster.Member;

import java.util.*;

/**
 * Cluster state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ClusterState implements Iterable<MemberState> {
  private final SortedMap<Integer, MemberState> members = new TreeMap<>();
  private final List<MemberState> activeMembers = new ArrayList<>();
  private final List<MemberState> passiveMembers = new ArrayList<>();

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
   * Adds a member to the active members list.
   */
  private void addActiveMember(MemberState member) {
    activeMembers.add(member);
    Collections.sort(activeMembers, (m1, m2) -> m1.getId() - m2.getId());
  }

  /**
   * Adds a member to the passive members list.
   */
  private void addPassiveMember(MemberState member) {
    passiveMembers.add(member);
    Collections.sort(passiveMembers, (m1, m2) -> m1.getId() - m2.getId());
  }

  /**
   * Sorts the active members.
   */
  private void sortActiveMembers() {
    Collections.sort(activeMembers, (m1, m2) -> m1.getId() - m2.getId());
    for (int i = 0; i < activeMembers.size(); i++) {
      activeMembers.get(i).setIndex(i);
    }
  }

  /**
   * Sorts the passive members.
   */
  private void sortPassiveMembers() {
    Collections.sort(passiveMembers, (m1, m2) -> m1.getId() - m2.getId());
    for (int i = 0; i < passiveMembers.size(); i++) {
      passiveMembers.get(i).setIndex(i);
    }
  }

  /**
   * Removes a member from the cluster state.
   *
   * @param member The member to remove.
   * @return The cluster state.
   */
  ClusterState removeMember(MemberState member) {
    members.remove(member.getId());
    if (member.getType() == Member.Type.ACTIVE) {
      removeActiveMember(member);
    } else {
      removePassiveMember(member);
    }
    return this;
  }

  /**
   * Removes a member from the active members list.
   */
  private void removeActiveMember(MemberState member) {
    Iterator<MemberState> iterator = activeMembers.iterator();
    while (iterator.hasNext()) {
      if (iterator.next().getId() == member.getId()) {
        iterator.remove();
      }
    }
  }

  /**
   * Removes a member from the passive members list.
   */
  private void removePassiveMember(MemberState member) {
    Iterator<MemberState> iterator = passiveMembers.iterator();
    while (iterator.hasNext()) {
      if (iterator.next().getId() == member.getId()) {
        iterator.remove();
      }
    }
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
   * Returns a sorted list of active members.
   *
   * @return A sorted list of active members.
   */
  List<MemberState> getActiveMembers() {
    return activeMembers;
  }

  /**
   * Returns a sorted list of passive members.
   *
   * @return A sorted list of passive members.
   */
  List<MemberState> getPassiveMembers() {
    return passiveMembers;
  }

  @Override
  public Iterator<MemberState> iterator() {
    return new ClusterStateIterator(members.entrySet().iterator());
  }

  /**
   * Cluster state iterator.
   */
  private class ClusterStateIterator implements Iterator<MemberState> {
    private final Iterator<Map.Entry<Integer, MemberState>> iterator;
    private MemberState member;

    private ClusterStateIterator(Iterator<Map.Entry<Integer, MemberState>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public MemberState next() {
      member = iterator.next().getValue();
      return member;
    }

    @Override
    public void remove() {
      iterator.remove();
      removeMember(member);
    }
  }

}
