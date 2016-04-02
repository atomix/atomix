/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.internal;

import java.util.*;

/**
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class MembersState implements Iterable<MemberState>, AutoCloseable {
  private final Map<String, MemberState> membersMap = new HashMap<>();
  private final List<MemberState> membersList = new ArrayList<>();

  /**
   * Adds a member to the set of members.
   */
  void add(MemberState member) {
    membersMap.put(member.id(), member);
    membersList.add(member);
  }

  /**
   * Gets a member by ID.
   */
  MemberState get(String id) {
    return membersMap.get(id);
  }

  /**
   * Gets a member by index.
   */
  MemberState get(int index) {
    return membersList.get(index);
  }

  /**
   * Removes a member by ID.
   */
  MemberState remove(String id) {
    MemberState member = membersMap.remove(id);
    if (member != null) {
      membersList.remove(member);
    }
    return member;
  }

  /**
   * Returns the size of the members list.
   */
  int size() {
    return membersList.size();
  }

  /**
   * Returns a boolean indicating whether the set of members is empty.
   */
  boolean isEmpty() {
    return membersList.isEmpty();
  }

  @Override
  public Iterator<MemberState> iterator() {
    return new MembersStateIterator(membersList.iterator());
  }

  @Override
  public void close() {
    membersList.forEach(MemberState::close);
  }

  /**
   * Members state iterator.
   */
  private class MembersStateIterator implements Iterator<MemberState> {
    private final Iterator<MemberState> iterator;
    private MemberState member;

    private MembersStateIterator(Iterator<MemberState> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public MemberState next() {
      member = iterator.next();
      return member;
    }

    @Override
    public void remove() {
      if (member != null) {
        iterator.remove();
        membersMap.remove(member.id());
      }
    }
  }

}
