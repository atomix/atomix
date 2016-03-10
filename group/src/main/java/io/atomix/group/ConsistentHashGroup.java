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
package io.atomix.group;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.hash.Hasher;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * {@link DistributedGroup} that consistently maps values to members on a ring.
 * <p>
 * Consistent hash groups place members of the parent {@link DistributedGroup} on a ring. Each member is hashed to
 * a point on the ring, and {@code n} virtual nodes for each concrete node are created to reduce hotspotting. When
 * accessing a member {@link #member(Object) by value}, the value is hashed to a point on the ring and the first
 * member following that point is returned.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ConsistentHashGroup extends SubGroup {

  /**
   * Returns a hash code for the given hash group arguments.
   */
  static int hashCode(int parent, Hasher hasher, int virtualNodes) {
    int hashCode = 17;
    hashCode = 37 * hashCode + parent;
    hashCode = 37 * hashCode + hasher.hashCode();
    hashCode = 37 * hashCode + virtualNodes;
    return hashCode;
  }

  private final GroupHashRing hashRing;
  private final Map<String, GroupMember> members = new ConcurrentHashMap<>();
  private final Listeners<GroupMember> joinListeners = new Listeners<>();
  private final Listeners<GroupMember> leaveListeners = new Listeners<>();

  ConsistentHashGroup(int subGroupId, MembershipGroup group, Collection<GroupMember> members, Hasher hasher, int virtualNodes) {
    super(subGroupId, group);
    this.hashRing = new GroupHashRing(hasher, virtualNodes, 1);
    for (GroupMember member : members) {
      this.members.put(member.id(), member);
      hashRing.addMember(member);
      election.onJoin(member);
    }
  }

  /**
   * Returns the member associated with the given value.
   * <p>
   * The given value's {@link Object#hashCode() hashCode} will be hashed to a point on the consistent hash ring.
   * The first {@link GroupMember} following that point on the ring will be returned. If there are no members in
   * the parent {@link DistributedGroup} then {@code null} will be returned.
   *
   * @param value The value for which to return the associated member.
   * @return The associated group member or {@code null} if the parent {@link DistributedGroup} is empty.
   * @throws NullPointerException if the value is {@code null}
   */
  public synchronized GroupMember member(Object value) {
    return hashRing.member(intToByteArray(value.hashCode()));
  }

  @Override
  public GroupMember member(String memberId) {
    return members.get(Assert.notNull(memberId, "memberId"));
  }

  @Override
  public Collection<GroupMember> members() {
    return members.values();
  }

  @Override
  public Listener<GroupMember> onJoin(Consumer<GroupMember> listener) {
    return joinListeners.add(listener);
  }

  @Override
  public Listener<GroupMember> onLeave(Consumer<GroupMember> listener) {
    return leaveListeners.add(listener);
  }

  /**
   * Converts an integer to a byte array.
   */
  private byte[] intToByteArray(int value) {
    return new byte[]{(byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value};
  }

  @Override
  protected synchronized void onJoin(GroupMember member) {
    GroupMember existing = members.get(member.id());
    if (existing != null) {
      if ((!(existing instanceof LocalGroupMember) && member instanceof LocalGroupMember) || (existing instanceof LocalGroupMember && !(member instanceof LocalGroupMember))) {
        hashRing.removeMember(existing);
        members.put(member.id(), member);
        hashRing.addMember(member);

        // Trigger election events.
        election.onJoin(member);

        // Trigger subgroup join events.
        for (SubGroup subGroup : subGroups.values()) {
          subGroup.onJoin(member);
        }
      } else {
        existing.setIndex(member.index());

        // Trigger election events.
        election.onJoin(existing);

        // Trigger subgroup join events.
        for (SubGroup subGroup : subGroups.values()) {
          subGroup.onJoin(existing);
        }
      }
    } else {
      members.put(member.id(), member);

      // Trigger join event listeners.
      joinListeners.accept(member);

      // Trigger election events.
      election.onJoin(member);

      // Trigger subgroup join events.
      for (SubGroup subGroup : subGroups.values()) {
        subGroup.onJoin(member);
      }
    }
  }

  @Override
  protected synchronized void onLeave(GroupMember member) {
    GroupMember removed = members.remove(member.id());
    if (removed != null) {
      leaveListeners.accept(removed);
    }
  }

}
