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

import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Group partition.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupPartition extends SubGroup {
  private final int partition;
  private final Map<String, GroupMember> members = new ConcurrentHashMap<>();
  private final List<GroupMember> sortedMembers;
  private final Listeners<GroupMember> joinListeners = new Listeners<>();
  private final Listeners<GroupMember> leaveListeners = new Listeners<>();
  private final Listeners<GroupPartitionMigration> migrationListeners = new Listeners<>();

  GroupPartition(int subGroupId, MembershipGroup group, int level, List<GroupMember> members, int partition) {
    super(subGroupId, group, level);
    this.sortedMembers = members;
    for (GroupMember member : members) {
      this.members.put(member.id(), member);
    }
    this.partition = partition;
  }

  /**
   * Returns the partition ID.
   *
   * @return The partition ID.
   */
  public int id() {
    return partition;
  }

  /**
   * Returns the member for the given index.
   *
   * @param index The member index.
   * @return The group member for the given index.
   */
  public GroupMember member(int index) {
    return sortedMembers.get(index);
  }

  @Override
  public GroupMember member(String memberId) {
    return members.get(memberId);
  }

  @Override
  public List<GroupMember> members() {
    return sortedMembers;
  }

  /**
   * Registers a partition migration listener.
   *
   * @param callback The callback to be called when a partition is migrated.
   * @return The partition migration listener.
   */
  public Listener<GroupPartitionMigration> onMigration(Consumer<GroupPartitionMigration> callback) {
    return migrationListeners.add(callback);
  }

  @Override
  public Listener<GroupMember> onJoin(Consumer<GroupMember> listener) {
    return joinListeners.add(listener);
  }

  @Override
  public Listener<GroupMember> onLeave(Consumer<GroupMember> listener) {
    return leaveListeners.add(listener);
  }

  @Override
  protected void onJoin(GroupMember member) {
  }

  @Override
  protected void onLeave(GroupMember member) {
  }

  /**
   * Updates the partition with the given group members.
   */
  void handleRepartition(List<GroupMember> members) {
    // Create a list of members that have joined the partition.
    List<GroupMember> joins = new ArrayList<>();
    for (GroupMember member : members) {
      if (!this.members.containsKey(member.id())) {
        joins.add(member);
      }
    }

    // Create a list of members that have left the partition.
    List<GroupMember> leaves = new ArrayList<>();
    for (GroupMember member : this.members.values()) {
      if (!members.contains(member)) {
        leaves.add(member);
      }
    }

    // Remove left members from the group and trigger listeners and children.
    for (GroupMember leave : leaves) {
      this.members.remove(leave.id());
      this.sortedMembers.remove(leave);
      leaveListeners.accept(leave);
      for (SubGroup child : children) {
        child.onLeave(leave);
      }
    }

    // Add joined members to the group and trigger listeners and children.
    for (GroupMember join : joins) {
      this.members.put(join.id(), join);
      this.sortedMembers.add(join);
      joinListeners.accept(join);
      for (SubGroup child : children) {
        child.onJoin(join);
      }
    }
  }

  /**
   * Handles a partition migration.
   */
  void handleMigration(GroupPartitionMigration migration) {
    migrationListeners.accept(migration);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d]", getClass().getSimpleName(), partition);
  }

}
