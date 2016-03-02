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
 * Distributed group hash ring.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class GroupHashRing {
  private final Hasher hasher;
  private final int virtualNodes;
  private final int replicationFactor;
  private final TreeMap<Long, GroupMember> ring = new TreeMap<>();

  public GroupHashRing(Hasher hasher, int virtualNodes, int replicationFactor) {
    this.hasher = Assert.notNull(hasher, "hasher");
    this.virtualNodes = Assert.argNot(virtualNodes, virtualNodes < 0, "virtualNodes must be positive");
    this.replicationFactor = Assert.argNot(replicationFactor, replicationFactor <= 0, "replicationFactor must be positive");
  }

  /**
   * Adds a member to the hash ring.
   *
   * @param member The member to add.
   */
  void addMember(GroupMember member) {
    if (!ring.values().contains(member)) {
      for (int i = 0; i < virtualNodes; i++) {
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
      for (int i = 0; i < virtualNodes; i++) {
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

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  /**
   * Hash ring iterator.
   */
  private class HashRingIterator implements Iterator<GroupMember> {
    private final Map.Entry<Long, GroupMember> firstEntry;
    private final Set<GroupMember> members = new HashSet<>();
    private Map.Entry<Long, GroupMember> next;
    private int count;

    private HashRingIterator(byte[] key) {
      long hash = hasher.hash64(key);
      Map.Entry<Long, GroupMember> firstEntry = ring.ceilingEntry(hash);
      if (firstEntry == null) {
        firstEntry = ring.firstEntry();
      }
      this.firstEntry = firstEntry;

      this.next = firstEntry;
      if (next != null) {
        members.add(next.getValue());
        count = 1;
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

      // If the replication factor has already been met, return the next (last) element.
      if (count == replicationFactor) {
        return entry.getValue();
      }

      // Get the next entry in the ring. If the entry is null, go back to the start.
      // If the next entry is equivalent to any iterated group member, skip it.
      next = ring.higherEntry(entry.getKey());
      while (next == null || (members.contains(next.getValue()) && !next.getKey().equals(firstEntry.getKey()))) {
        if (next == null) {
          next = ring.firstEntry();
        } else {
          next = ring.higherEntry(next.getKey());
        }
      }

      // If the next entry is equal to the first entry, that indicates that the total number of members
      // available is less than the replication factor. Reset the members list to add the same members again.
      if (next.getKey().equals(firstEntry.getKey())) {
        members.clear();
      }

      members.add(next.getValue());
      count++;

      return entry.getValue();
    }
  }

}
