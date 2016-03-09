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

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.hash.Hasher;
import io.atomix.catalyst.util.hash.Murmur2Hasher;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.group.state.GroupCommands;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.ResourceType;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Distributed membership group.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MembershipGroup extends AbstractResource<DistributedGroup> implements DistributedGroup {
  private final Listeners<GroupMember> joinListeners = new Listeners<>();
  private final Listeners<GroupMember> leaveListeners = new Listeners<>();
  private final Set<String> joining = new CopyOnWriteArraySet<>();
  private final DistributedGroup.Options options;
  private final Address address;
  private final Server server;
  final GroupConnectionManager connections;
  private final GroupProperties properties = new GroupProperties(null, this);
  private final GroupElection election = new GroupElection(0, this);
  private final GroupTaskQueue tasks = new GroupTaskQueue(this);
  final Map<String, GroupMember> members = new ConcurrentHashMap<>();
  private final Set<SubGroup> children = new HashSet<>();
  final Map<Integer, SubGroup> groups = new ConcurrentHashMap<>();

  public MembershipGroup(CopycatClient client, Properties options) {
    super(client, new ResourceType(DistributedGroup.class), options);
    this.options = new DistributedGroup.Options(Assert.notNull(options, "options"));
    this.address = this.options.getAddress();
    this.server = client.transport().server();
    this.connections = new GroupConnectionManager(client.transport().client(), client.context());
  }

  @Override
  public DistributedGroup.Config config() {
    return new DistributedGroup.Config(config);
  }

  @Override
  public DistributedGroup.Options options() {
    return options;
  }

  @Override
  public GroupProperties properties() {
    return properties;
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
  public synchronized ConsistentHashGroup hash(Hasher hasher, int virtualNodes) {
    int subGroupId = ConsistentHashGroup.hashCode(1, hasher, virtualNodes);
    SubGroup group = groups.get(subGroupId);
    if (group == null) {
      group = new ConsistentHashGroup(subGroupId, this, 1, members(), hasher, virtualNodes);
      groups.put(subGroupId, group);
      children.add(group);
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
  public synchronized PartitionGroup partition(int partitions, int replicationFactor, GroupPartitioner partitioner) {
    int subGroupId = PartitionGroup.hashCode(1, partitions, replicationFactor, partitioner);
    SubGroup group = groups.get(subGroupId);
    if (group == null) {
      group = new PartitionGroup(subGroupId, this, 1, members(), partitions, replicationFactor, partitioner);
      groups.put(subGroupId, group);
      children.add(group);
    }
    return (PartitionGroup) group;
  }

  @Override
  public GroupMember member(String memberId) {
    return members.get(memberId);
  }

  @Override
  public Collection<GroupMember> members() {
    return members.values();
  }

  @Override
  public CompletableFuture<LocalGroupMember> join() {
    return join(UUID.randomUUID().toString(), false);
  }

  @Override
  public CompletableFuture<LocalGroupMember> join(String memberId) {
    return join(memberId, true);
  }

  /**
   * Joins the group.
   *
   * @param memberId The member ID with which to join the group.
   * @param persistent Indicates whether the member ID is persistent.
   * @return A completable future to be completed once the member has joined the group.
   */
  private CompletableFuture<LocalGroupMember> join(String memberId, boolean persistent) {
    joining.add(memberId);
    return submit(new GroupCommands.Join(memberId, address, persistent)).whenComplete((result, error) -> {
      if (error != null) {
        joining.remove(memberId);
      }
    }).thenApply(info -> {
      LocalGroupMember member = (LocalGroupMember) members.get(info.memberId());
      if (member == null) {
        member = new LocalGroupMember(info, this);
        members.put(info.memberId(), member);
      }
      return member;
    });
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
  public CompletableFuture<DistributedGroup> open() {
    return client.connect().thenApply(result -> {
      client.onEvent("join", this::onJoinEvent);
      client.onEvent("leave", this::onLeaveEvent);
      client.onEvent("term", election::onTermEvent);
      client.onEvent("elect", election::onElectEvent);
      client.onEvent("resign", election::onResignEvent);
      client.onEvent("task", this::onTaskEvent);
      client.onEvent("ack", this::onAckEvent);
      client.onEvent("fail", this::onFailEvent);
      return result;
    }).thenCompose(v -> listen())
      .thenCompose(v -> sync())
      .thenApply(v -> this);
  }

  /**
   * Starts the server.
   */
  private CompletableFuture<Void> listen() {
    if (address != null) {
      return server.listen(address, c -> {
        c.handler(GroupMessage.class, this::onMessage);
      });
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Handles a group message.
   */
  private CompletableFuture<Object> onMessage(GroupMessage message) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    GroupMember member = members.get(message.member());
    if (member != null) {
      if (member instanceof LocalGroupMember) {
        ((LocalGroupMember) member).connection().handleMessage(message.setFuture(future));
      } else {
        future.completeExceptionally(new IllegalStateException("not a local member"));
      }
    } else {
      future.completeExceptionally(new IllegalStateException("unknown member"));
    }
    return future;
  }

  /**
   * Synchronizes the membership group.
   */
  private CompletableFuture<Void> sync() {
    return submit(new GroupCommands.Listen()).thenAccept(members -> {
      for (GroupMemberInfo info : members) {
        GroupMember member = this.members.get(info.memberId());
        if (member == null) {
          member = new GroupMember(info, this);
          this.members.put(member.id(), member);
        }
      }
    });
  }

  /**
   * Handles a join event received from the cluster.
   */
  private void onJoinEvent(GroupMemberInfo info) {
    GroupMember member;
    if (joining.contains(info.memberId())) {
      member = new LocalGroupMember(info, this);
      members.put(info.memberId(), member);
      for (Listener<GroupMember> listener : joinListeners) {
        listener.accept(member);
      }
      for (SubGroup child : children) {
        child.onJoin(member);
      }
    } else {
      member = members.get(info.memberId());
      if (member == null) {
        member = new GroupMember(info, this);
        members.put(info.memberId(), member);
        for (Listener<GroupMember> listener : joinListeners) {
          listener.accept(member);
        }
        for (SubGroup child : children) {
          child.onJoin(member);
        }
      }
    }
  }

  /**
   * Handles a leave event received from the cluster.
   */
  private void onLeaveEvent(String memberId) {
    GroupMember member = members.remove(memberId);
    if (member != null) {
      for (Listener<GroupMember> listener : leaveListeners) {
        listener.accept(member);
      }
      for (SubGroup child : children) {
        child.onLeave(member);
      }
    }
  }

  /**
   * Handles a task event received from the cluster.
   */
  private void onTaskEvent(GroupTask task) {
    GroupMember localMember = members.get(task.member());
    if (localMember != null && localMember instanceof LocalGroupMember) {
      CompletableFuture<Boolean> future = new CompletableFuture<>();
      future.whenComplete((succeeded, error) -> {
        if (error == null && succeeded) {
          submit(new GroupCommands.Ack(0, task.member(), task.id(), true));
        } else {
          submit(new GroupCommands.Ack(0, task.member(), task.id(), false));
        }
      });
      ((LocalGroupMember) localMember).tasks().onTask(task.setFuture(future));
    }
  }

  /**
   * Handles an ack event received from the cluster.
   */
  private void onAckEvent(GroupCommands.Submit submit) {
    if (submit.member() != null) {
      GroupMember member = members.get(submit.member());
      if (member != null) {
        member.tasks().onAck(submit.id());
      }
    } else {
      tasks.onAck(submit.id());
    }
  }

  /**
   * Handles a fail event received from the cluster.
   */
  private void onFailEvent(GroupCommands.Submit submit) {
    if (submit.member() != null) {
      GroupMember member = members.get(submit.member());
      if (member != null) {
        member.tasks().onFail(submit.id());
      }
    } else {
      tasks.onFail(submit.id());
    }
  }

  /**
   * Submits a query to the cluster.
   */
  protected <T> CompletableFuture<T> submit(Query<T> query) {
    return super.submit(query);
  }

  /**
   * Submits a command to the cluster.
   */
  protected <T> CompletableFuture<T> submit(Command<T> command) {
    return super.submit(command);
  }

}
