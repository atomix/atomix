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
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Group partitions.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupPartitions implements Iterable<GroupPartition> {
  final List<GroupPartition> partitions = new ArrayList<>();
  GroupPartitioner partitioner = (value, partitions) -> -1;
  private final Listeners<GroupPartitionMigration> migrationListeners = new Listeners<>();

  GroupPartitions() {
  }

  /**
   * Returns a partition by ID.
   *
   * @param partitionId The partition ID.
   * @return The group partition.
   * @throws IndexOutOfBoundsException if the given {@code partitionId} is greater than the range of partitions in the group
   */
  public GroupPartition partition(int partitionId) {
    return partitions.get(partitionId);
  }

  /**
   * Returns a partition for the given value.
   *
   * @param value The value for which to return a partition.
   * @return The partition for the given value or {@code null} if the value was not mapped to any partition.
   */
  public GroupPartition partition(Object value) {
    int partitionId = partitioner.partition(value, partitions.size());
    return partitionId != -1 ? partitions.get(partitionId) : null;
  }

  /**
   * Returns an ordered list of partitions in the group.
   *
   * @return A list of partitions in the group. The position of each partition in the returned {@link List} is the partition's unique ID.
   */
  public List<GroupPartition> partitions() {
    return partitions;
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

  /**
   * Handles a partition migration.
   */
  void handleMigration(GroupPartitionMigration migration) {
    migrationListeners.accept(migration);
    migration.partition().handleMigration(migration);
  }

  @Override
  public Iterator<GroupPartition> iterator() {
    return partitions.iterator();
  }

  @Override
  public String toString() {
    return String.format("%s[partitions=%d]", getClass().getSimpleName(), partitions.size());
  }

}
