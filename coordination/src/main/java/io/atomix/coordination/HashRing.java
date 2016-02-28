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
package io.atomix.coordination;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.hash.Hasher;

import java.util.*;

/**
 * Hash ring.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class HashRing {
  private final Hasher hasher;
  private final int virtualMembers;
  private final int replicationFactor;
  private final TreeMap<Long, GroupMember> ring = new TreeMap<>();

  public HashRing(Hasher hasher, int virtualMembers, int replicationFactor) {
    this.hasher = Assert.notNull(hasher, "hasher");
    this.virtualMembers = Assert.argNot(virtualMembers, virtualMembers < 0, "virtualMembers must be positive");
    this.replicationFactor = Assert.argNot(replicationFactor, replicationFactor <= 0, "replicationFactor must be positive");
  }

  /**
   * Adds a member to the hash ring.
   *
   * @param member The member to add.
   */
  void addMember(GroupMember member) {
    if (!ring.values().contains(member)) {
      for (int i = 0; i < 1 + virtualMembers; i++) {
        ring.put(hasher.hash64(member.id() + i), member);
      }
    }
  }

  /**
   * Removes a member from the hash ring.
   *
   * @param member The member to remove.
   */
  void removeMember(GroupMember member) {
    if (ring.values().contains(member)) {
      for (int i = 0; i < 1 + virtualMembers; i++) {
        ring.remove(hasher.hash64(member.id() + i));
      }
    }
  }

  /**
   * Hashes the given key to a group member.
   *
   * @param key The key to hash.
   * @return The group member to which the key maps.
   */
  public GroupMember member(byte[] key) {
    if (ring.isEmpty())
      return null;

    long hash = hasher.hash64(key);
    Map.Entry<Long, GroupMember> entry = ring.ceilingEntry(hash);
    if (entry == null) {
      return ring.firstEntry().getValue();
    }
    return entry.getValue();
  }

  /**
   * Hashes the given key to a set of group members.
   *
   * @param key The key to hash.
   * @return The group members to which the key hashes according to the given replication factor.
   */
  public List<GroupMember> members(byte[] key) {
    List<GroupMember> members = new ArrayList<>();
    Iterator<GroupMember> iterator = iterator(key);
    while (iterator.hasNext()) {
      members.add(iterator.next());
    }
    return members;
  }

  /**
   * Returns an iterator for the given key.
   *
   * @param key The key for which to return an iterator.
   * @return The member iterator.
   */
  public Iterator<GroupMember> iterator(byte[] key) {
    return new HashRingIterator(key);
  }

  /**
   * Hash ring iterator.
   */
  private class HashRingIterator implements Iterator<GroupMember> {
    private final Map.Entry<Long, GroupMember> firstEntry;
    private final Set<String> members = new HashSet<>();
    private Map.Entry<Long, GroupMember> next;

    private HashRingIterator(byte[] key) {
      long hash = hasher.hash64(key);
      Map.Entry<Long, GroupMember> firstEntry = ring.ceilingEntry(hash);
      if (firstEntry == null) {
        firstEntry = ring.firstEntry();
      }
      this.firstEntry = firstEntry;

      this.next = firstEntry;
      if (next != null) {
        members.add(next.getValue().id());
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public GroupMember next() {
      Map.Entry<Long, GroupMember> entry = next;
      if (entry == null) {
        throw new NoSuchElementException();
      }

      next = null;

      // If the replication factor has already been met, return the next element.
      if (members.size() == replicationFactor) {
        return entry.getValue();
      }

      // Get the next entry in the ring. If the entry is null, go back to the start.
      // If the next entry is equivalent to any iterated group member, skip it.
      next = ring.higherEntry(entry.getKey());
      while (next == null || members.contains(next.getValue().id())) {
        if (next == null) {
          next = ring.firstEntry();
        } else {
          next = ring.higherEntry(next.getKey());
        }
      }

      // If the next entry is equal to the starting entry, set the next entry to null.
      if (next.getKey().equals(firstEntry.getKey())) {
        next = null;
      } else {
        members.add(next.getValue().id());
      }
      return entry.getValue();
    }
  }

}
