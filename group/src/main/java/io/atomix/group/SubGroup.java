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
import io.atomix.resource.ReadConsistency;
import io.atomix.resource.ResourceType;
import io.atomix.resource.WriteConsistency;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Abstract distributed group.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class SubGroup implements DistributedGroup {
  protected final MembershipGroup group;
  protected final int groupId;
  private final int level;
  protected final GroupElection election;
  protected final GroupTaskQueue tasks;
  protected final Set<SubGroup> children = new CopyOnWriteArraySet<>();

  protected SubGroup(MembershipGroup group, int groupId, int level) {
    this.group = Assert.notNull(group, "group");
    this.groupId = groupId;
    this.level = level;
    this.election = new GroupElection(groupId, group);
    this.tasks = new SubGroupTaskQueue(group, this);
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
  public GroupElection election() {
    return election;
  }

  @Override
  public GroupTaskQueue tasks() {
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
    int hashCode = ConsistentHashGroup.hashCode(level+1, hasher, virtualNodes);
    SubGroup group = this.group.groups.get(hashCode);
    if (group == null) {
      synchronized (this.group) {
        group = this.group.groups.get(hashCode);
        if (group == null) {
          group = new ConsistentHashGroup(this.group, hashCode, level+1, members(), hasher, virtualNodes);
          this.group.groups.put(hashCode, group);
          children.add(group);
        }
      }
    }
    return (ConsistentHashGroup) group;
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
  public PartitionGroup partition(int partitions, GroupPartitioner partitioner) {
    return partition(partitions, 1, partitioner);
  }

  @Override
  public PartitionGroup partition(int partitions, int replicationFactor, GroupPartitioner partitioner) {
    int hashCode = PartitionGroup.hashCode(level+1, partitions, replicationFactor, partitioner);
    SubGroup group = this.group.groups.get(hashCode);
    if (group == null) {
      synchronized (this.group) {
        group = this.group.groups.get(hashCode);
        if (group == null) {
          group = new PartitionGroup(this.group, hashCode, level+1, members(), partitions, replicationFactor, partitioner);
          this.group.groups.put(hashCode, group);
          children.add(group);
        }
      }
    }
    return (PartitionGroup) group;
  }

  @Override
  public CompletableFuture<LocalGroupMember> join() {
    return group.join();
  }

  @Override
  public CompletableFuture<LocalGroupMember> join(String memberId) {
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
    return groupId;
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getSimpleName(), members());
  }

}
