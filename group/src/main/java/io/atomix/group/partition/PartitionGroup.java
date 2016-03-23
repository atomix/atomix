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
package io.atomix.group.partition;

import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.hash.Murmur2Hasher;
import io.atomix.group.*;
import io.atomix.group.util.HashRing;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Group partitions.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class PartitionGroup extends SubGroup {
  private final Map<String, GroupMember> members = new ConcurrentHashMap<>();
  private final Partitions partitions;
  private final HashRing hashRing;
  private final Listeners<GroupMember> joinListeners = new Listeners<>();
  private final Listeners<GroupMember> leaveListeners = new Listeners<>();
  private final Listeners<PartitionMigration> migrationListeners = new Listeners<>();

  public PartitionGroup(int subGroupId, MembershipGroup group, Collection<GroupMember> members, int numPartitions, int replicationFactor, Partitioner partitioner) {
    super(subGroupId, group);
    this.hashRing = new HashRing(new Murmur2Hasher(), 100, replicationFactor);
    for (GroupMember member : members) {
      hashRing.addMember(member);
      election.onJoin(member);
    }

    List<Partition> partitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(new Partition(Partition.hashCode(subGroupId, i), group, hashRing.members(intToByteArray(i)), i));
    }
    this.partitions = new Partitions(partitions, partitioner);
  }

  @Override
  public GroupMember member(String memberId) {
    return members.get(memberId);
  }

  @Override
  public Collection<GroupMember> members() {
    return members.values();
  }

  /**
   * Returns an ordered list of partitions in the group.
   *
   * @return A list of partitions in the group. The position of each partition in the returned {@link List} is the partition's unique ID.
   */
  public Partitions partitions() {
    return partitions;
  }

  /**
   * Registers a partition migration listener.
   *
   * @param callback The callback to be called when a partition is migrated.
   * @return The partition migration listener.
   */
  public Listener<PartitionMigration> onMigration(Consumer<PartitionMigration> callback) {
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
    GroupMember existing = members.get(member.id());
    if (existing != null) {
      // If the member changed from local to remote or vice-versa, update the member object.
      if ((!(existing instanceof LocalMember) && member instanceof LocalMember) || (existing instanceof LocalMember && !(member instanceof LocalMember))) {
        hashRing.removeMember(existing);
        members.put(member.id(), member);
        hashRing.addMember(member);

        // Trigger election events.
        election.onJoin(member);

        // Trigger subgroup join events.
        for (SubGroupController subGroup : subGroups.values()) {
          subGroup.onJoin(member);
        }
      } else {
        // Trigger election events.
        election.onJoin(existing);

        // Trigger subgroup join events.
        for (SubGroupController subGroup : subGroups.values()) {
          subGroup.onJoin(existing);
        }
      }
    } else {
      members.put(member.id(), member);

      // Calculate old and new partitions given the member change.
      List<List<GroupMember>> oldPartitions = getOldPartitions();
      hashRing.addMember(member);
      List<List<GroupMember>> newPartitions = getNewPartitions();

      // Migrate partition members.
      migratePartitionMembers(oldPartitions, newPartitions);

      // Trigger join event listeners.
      joinListeners.accept(member);

      // Trigger election events.
      election.onJoin(member);

      // Trigger subgroup join events.
      for (SubGroupController subGroup : subGroups.values()) {
        subGroup.onJoin(member);
      }
    }
  }

  @Override
  protected void onLeave(GroupMember member) {
    GroupMember removed = members.remove(member.id());
    if (removed != null) {
      // Calculate old and new partitions given the member change.
      List<List<GroupMember>> oldPartitions = getOldPartitions();
      hashRing.removeMember(member);
      List<List<GroupMember>> newPartitions = getNewPartitions();

      // Migrate partition members.
      migratePartitionMembers(oldPartitions, newPartitions);

      // Trigger subgroup leave events.
      for (SubGroupController subGroup : subGroups.values()) {
        subGroup.onLeave(removed);
      }

      // Trigger leave listeners.
      leaveListeners.accept(removed);
    }
  }

  /**
   * Returns a list of old partition members.
   */
  private List<List<GroupMember>> getOldPartitions() {
    List<List<GroupMember>> partitions = new ArrayList<>();
    for (Partition partition : this.partitions) {
      partitions.add(partition.members());
    }
    return partitions;
  }

  /**
   * Returns a list of new partition members.
   */
  private List<List<GroupMember>> getNewPartitions() {
    List<List<GroupMember>> partitions = new ArrayList<>();
    for (int i = 0; i < this.partitions.size(); i++) {
      partitions.add(hashRing.members(intToByteArray(i)));
    }
    return partitions;
  }

  /**
   * Migrates partitions from the old partitions to the new partitions.
   */
  private void migratePartitionMembers(List<List<GroupMember>> oldPartitions, List<List<GroupMember>> newPartitions) {
    // Iterate through each of the partitions in the group.
    for (int i = 0; i < partitions.size(); i++) {
      // Get a list of the old and new partition members.
      List<GroupMember> oldPartitionMembers = oldPartitions.get(i);
      List<GroupMember> newPartitionMembers = newPartitions.get(i);

      List<PartitionMigration> migrations = new ArrayList<>();
      Set<GroupMember> migratedMembers = new HashSet<>();
      if (!oldPartitionMembers.equals(newPartitionMembers)) {

        // Determine the members for which the partition changed.
        for (GroupMember oldMember : oldPartitionMembers) {
          if (!migratedMembers.contains(oldMember)) {
            for (GroupMember newMember : newPartitionMembers) {
              if (!migratedMembers.contains(newMember)) {
                migrations.add(new PartitionMigration(oldMember, newMember, partitions.get(i)));
                migratedMembers.add(oldMember);
                migratedMembers.add(newMember);
              }
            }
          }
        }

        // Determine the members present in old partition members but not in new.
        for (GroupMember oldMember : oldPartitionMembers) {
          if (!migratedMembers.contains(oldMember)) {
            migrations.add(new PartitionMigration(oldMember, null, partitions.get(i)));
            migratedMembers.add(oldMember);
          }
        }

        // Determine the members present in new partition members but not old.
        for (GroupMember newMember : newPartitionMembers) {
          if (!migratedMembers.contains(newMember) && !migratedMembers.contains(newMember)) {
            migratedMembers.add(newMember);
            migrations.add(new PartitionMigration(null, newMember, partitions.get(i)));
          }
        }
      }

      // Update the partition members and trigger migration callbacks.
      partitions.get(i).handleRepartition(newPartitions.get(i));
      for (PartitionMigration migration : migrations) {
        migrationListeners.accept(migration);
        migration.partition().handleMigration(migration);
      }
    }
  }

  /**
   * Converts an integer to a byte array.
   */
  private byte[] intToByteArray(int value) {
    return new byte[]{(byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value};
  }

  @Override
  public String toString() {
    return String.format("%s[partitions=%d]", getClass().getSimpleName(), partitions.size());
  }

}
