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

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.catalyst.util.hash.Hasher;
import io.atomix.catalyst.util.hash.Murmur2Hasher;
import io.atomix.group.election.Election;
import io.atomix.group.election.ElectionController;
import io.atomix.group.partition.*;
import io.atomix.group.tasks.SubGroupTaskQueue;
import io.atomix.group.tasks.TaskQueue;
import io.atomix.group.util.GroupIdGenerator;
import io.atomix.resource.ReadConsistency;
import io.atomix.resource.ResourceType;
import io.atomix.resource.WriteConsistency;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Base class for subgroups of {@link DistributedGroup}.
 * <p>
 * {@link DistributedGroup} can be partitioned into subgroups that can be nested to any depth. This allows groups
 * to be partitioned multiple times to facilitate replication algorithms. Subgroups are guaranteed to be consistent
 * across all nodes in a cluster. For example, in a {@link MembershipGroup} partitioned into a {@link PartitionGroup}
 * with {@code 3} partitions, each {@link Partition} will represent the same members on all nodes in the cluster.
 * Changes to groups and subgroups are guaranteed to occur in the same order on all nodes.
 * <p>
 * Subgroups inherit a number of attributes of their parent group. When a group is {@link #partition(int) partitioned}
 * into a subgroup, the subgroup will inherit the {@link #members() membership} list of the parent group but may
 * represent only a subset of those members. Changes in the set of members in a parent group will be immediately
 * reflected in all subgroups. Subgroups inherit the {@link GroupProperties properties}, {@link TaskQueue tasks},
 * {@link io.atomix.group.DistributedGroup.Config configuration}, and {@link io.atomix.group.DistributedGroup.Options options}
 * of the base {@link MembershipGroup}.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class SubGroup implements DistributedGroup {
  protected final int subGroupId;
  protected final MembershipGroup group;
  protected final ElectionController election = new ElectionController(this);
  protected final TaskQueue tasks;
  protected final Map<Integer, SubGroupController> subGroups = new ConcurrentHashMap<>();

  protected SubGroup(int subGroupId, MembershipGroup group) {
    this.group = Assert.notNull(group, "group");
    this.subGroupId = subGroupId;
    this.tasks = new SubGroupTaskQueue(this, group);
  }

  @Override
  public ResourceType type() {
    return group.type();
  }

  @Override
  public Serializer serializer() {
    return group.serializer();
  }

  @Override
  public ThreadContext context() {
    return group.context();
  }

  @Override
  public Config config() {
    return group.config();
  }

  @Override
  public Options options() {
    return group.options();
  }

  @Override
  public State state() {
    return group.state();
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> callback) {
    return group.onStateChange(callback);
  }

  @Override
  public WriteConsistency writeConsistency() {
    return group.writeConsistency();
  }

  @Override
  public DistributedGroup with(WriteConsistency consistency) {
    group.with(consistency);
    return this;
  }

  @Override
  public ReadConsistency readConsistency() {
    return group.readConsistency();
  }

  @Override
  public DistributedGroup with(ReadConsistency consistency) {
    group.with(consistency);
    return this;
  }

  @Override
  public GroupProperties properties() {
    return group.properties();
  }

  @Override
  public Election election() {
    return election.election();
  }

  @Override
  public TaskQueue tasks() {
    return tasks;
  }

  @Override
  public ConsistentHashGroup hash() {
    return hash(new Murmur2Hasher(), 100);
  }

  @Override
  public ConsistentHashGroup hash(Hasher hasher) {
    return hash(hasher, 100);
  }

  @Override
  public ConsistentHashGroup hash(int virtualNodes) {
    return hash(new Murmur2Hasher(), virtualNodes);
  }

  @Override
  public ConsistentHashGroup hash(Hasher hasher, int virtualNodes) {
    int subGroupId = GroupIdGenerator.groupIdFor(this.subGroupId, hasher, virtualNodes);
    return (ConsistentHashGroup) subGroups.computeIfAbsent(subGroupId, s -> new SubGroupController(new ConsistentHashGroup(subGroupId, this.group, members(), hasher, virtualNodes))).group();
  }

  @Override
  public PartitionGroup partition(int partitions) {
    return partition(partitions, 1, new HashPartitioner());
  }

  @Override
  public PartitionGroup partition(int partitions, int replicationFactor) {
    return partition(partitions, replicationFactor, new HashPartitioner());
  }

  @Override
  public PartitionGroup partition(int partitions, Partitioner partitioner) {
    return partition(partitions, 1, partitioner);
  }

  @Override
  public PartitionGroup partition(int partitions, int replicationFactor, Partitioner partitioner) {
    int subGroupId = GroupIdGenerator.groupIdFor(this.subGroupId, partitions, replicationFactor, partitioner);
    return (PartitionGroup) subGroups.computeIfAbsent(subGroupId, s -> new SubGroupController(new PartitionGroup(subGroupId, this.group, members(), partitions, replicationFactor, partitioner))).group();
  }

  @Override
  public CompletableFuture<LocalMember> join() {
    return group.join();
  }

  @Override
  public CompletableFuture<LocalMember> join(String memberId) {
    return group.join(memberId);
  }

  @Override
  public CompletableFuture<DistributedGroup> open() {
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isOpen() {
    return group.isOpen();
  }

  @Override
  public boolean isClosed() {
    return group.isClosed();
  }

  protected abstract void onJoin(GroupMember member);

  protected abstract void onLeave(GroupMember member);

  @Override
  public int hashCode() {
    return subGroupId;
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getSimpleName(), members());
  }

}
