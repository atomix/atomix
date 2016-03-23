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
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.hash.Hasher;
import io.atomix.catalyst.util.hash.Murmur2Hasher;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.group.connection.ConnectionManager;
import io.atomix.group.connection.Message;
import io.atomix.group.election.Election;
import io.atomix.group.election.ElectionController;
import io.atomix.group.partition.ConsistentHashGroup;
import io.atomix.group.partition.HashPartitioner;
import io.atomix.group.partition.PartitionGroup;
import io.atomix.group.partition.Partitioner;
import io.atomix.group.state.GroupCommands;
import io.atomix.group.task.GroupTaskQueue;
import io.atomix.group.task.Task;
import io.atomix.group.task.TaskQueue;
import io.atomix.group.util.GroupIdGenerator;
import io.atomix.group.util.Submitter;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.ReadConsistency;
import io.atomix.resource.ResourceType;
import io.atomix.resource.WriteConsistency;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Base {@link DistributedGroup} implementation which manages a membership set for the group
 * and all {@link SubGroup}s.
 * <p>
 * The membership group is the base {@link DistributedGroup} type which is created when a new group
 * is created via the Atomix API.
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("foo").get();
 *   }
 * </pre>
 * The membership group controls the set of members available within the group and all {@link SubGroup}s.
 * When a membership change occurs within the group, the membership group will update its state and
 * the state of all subgroups.
 * <p>
 * Subgroups created by the membership group via either {@link #hash()} or {@link #partition(int)} will
 * inherit the membership group's {@link GroupProperties properties} and members. However, subgroups
 * may filter members according to their requirements.
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
  final ConnectionManager connections;
  private final GroupProperties properties = new GroupProperties(this);
  private final ElectionController election = new ElectionController(this);
  private final TaskQueue tasks = new GroupTaskQueue(this);
  final Map<String, GroupMember> members = new ConcurrentHashMap<>();
  private final Map<Integer, SubGroup> subGroups = new ConcurrentHashMap<>();
  private final Submitter submitter = new Submitter() {
    @Override
    public <T extends Command<U>, U> CompletableFuture<U> submit(T command) {
      return MembershipGroup.this.submit(command);
    }

    @Override
    public <T extends Command<U>, U> CompletableFuture<U> submit(T command, WriteConsistency consistency) {
      return MembershipGroup.this.submit(command, consistency);
    }

    @Override
    public <T extends Query<U>, U> CompletableFuture<U> submit(T query) {
      return MembershipGroup.this.submit(query);
    }

    @Override
    public <T extends Query<U>, U> CompletableFuture<U> submit(T query, ReadConsistency consistency) {
      return MembershipGroup.this.submit(query, consistency);
    }
  };

  public MembershipGroup(CopycatClient client, Properties options) {
    super(client, new ResourceType(DistributedGroup.class), options);
    this.options = new DistributedGroup.Options(Assert.notNull(options, "options"));
    this.address = this.options.getAddress();
    this.server = client.transport().server();
    this.connections = new ConnectionManager(client.transport().client(), client.context());
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
  public synchronized ConsistentHashGroup hash(Hasher hasher, int virtualNodes) {
    int subGroupId = GroupIdGenerator.groupIdFor(0, hasher, virtualNodes);
    return (ConsistentHashGroup) subGroups.computeIfAbsent(subGroupId, g -> new ConsistentHashGroup(g, this, members(), hasher, virtualNodes));
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
  public synchronized PartitionGroup partition(int partitions, int replicationFactor, Partitioner partitioner) {
    int subGroupId = GroupIdGenerator.groupIdFor(0, partitions, replicationFactor, partitioner);
    return (PartitionGroup) subGroups.computeIfAbsent(subGroupId, s -> new PartitionGroup(s, this, members(), partitions, replicationFactor, partitioner));
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
  public CompletableFuture<LocalMember> join() {
    return join(UUID.randomUUID().toString(), false);
  }

  @Override
  public CompletableFuture<LocalMember> join(String memberId) {
    return join(memberId, true);
  }

  /**
   * Joins the group.
   *
   * @param memberId The member ID with which to join the group.
   * @param persistent Indicates whether the member ID is persistent.
   * @return A completable future to be completed once the member has joined the group.
   */
  private CompletableFuture<LocalMember> join(String memberId, boolean persistent) {
    joining.add(memberId);
    return submit(new GroupCommands.Join(memberId, address, persistent)).whenComplete((result, error) -> {
      if (error != null) {
        joining.remove(memberId);
      }
    }).thenApply(info -> {
      LocalMember member = (LocalMember) members.get(info.memberId());
      if (member == null) {
        member = new LocalMember(info, this, submitter);
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
        c.handler(Message.class, this::onMessage);
      });
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Handles a group message.
   */
  private CompletableFuture<Object> onMessage(Message message) {
    GroupMember member = members.get(message.member());
    if (member == null) {
      return Futures.exceptionalFuture(new IllegalStateException("unknown member"));
    }

    if (member instanceof LocalMember) {
      return ((LocalMember) member).connection.onMessage(message);
    } else {
      return Futures.exceptionalFuture(new IllegalStateException("not a local member"));
    }
  }

  /**
   * Synchronizes the membership group.
   */
  private CompletableFuture<Void> sync() {
    return submit(new GroupCommands.Listen()).thenAccept(members -> {
      for (GroupMemberInfo info : members) {
        GroupMember member = this.members.get(info.memberId());
        if (member == null) {
          member = new GroupMember(info, this, submitter);
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
    if (joining.remove(info.memberId())) {
      member = new LocalMember(info, this, submitter);
      members.put(info.memberId(), member);

      // Trigger join listeners.
      joinListeners.accept(member);

      // Trigger elections.
      election.onJoin(member);

      // Trigger subgroup join listeners.
      for (SubGroup subGroup : subGroups.values()) {
        subGroup.onJoin(member);
      }
    } else {
      member = members.get(info.memberId());
      if (member == null) {
        member = new GroupMember(info, this, submitter);
        members.put(info.memberId(), member);

        // Trigger join listeners.
        joinListeners.accept(member);

        // Trigger elections.
        election.onJoin(member);

        // Trigger subgroup join listeners.
        for (SubGroup subGroup : subGroups.values()) {
          subGroup.onJoin(member);
        }
      } else {
        // If the joining member's index is greater than the existing member's index then the member
        // was reopened on another node. We need to reset elections.
        if (info.index() > member.version()) {
          // Update the member's index and trigger a new election if necessary.
          member.setIndex(info.index());
          election.onJoin(member);

          // Trigger subgroup join listeners.
          for (SubGroup subGroup : subGroups.values()) {
            subGroup.onJoin(member);
          }
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
      // Trigger subgroup leave listeners first to ensure the member leaves children before parents.
      for (SubGroup subGroup : subGroups.values()) {
        subGroup.onLeave(member);
      }

      // Trigger a new election.
      election.onLeave(member);

      // Trigger leave listeners.
      leaveListeners.accept(member);
    }
  }

  /**
   * Handles a task event received from the cluster.
   */
  private void onTaskEvent(Task task) {
    GroupMember localMember = members.get(task.member());
    if (localMember != null && localMember instanceof LocalMember) {
      ((LocalMember) localMember).tasks.onTask(task).whenComplete((succeeded, error) -> {
        if (error == null && succeeded) {
          submit(new GroupCommands.Ack(task.member(), task.id(), true));
        } else {
          submit(new GroupCommands.Ack(task.member(), task.id(), false));
        }
      });
    }
  }

  /**
   * Handles an ack event received from the cluster.
   */
  private void onAckEvent(GroupCommands.Submit submit) {
    GroupMember member = members.get(submit.member());
    if (member != null) {
      member.tasks.onAck(submit.id());
    }
  }

  /**
   * Handles a fail event received from the cluster.
   */
  private void onFailEvent(GroupCommands.Submit submit) {
    GroupMember member = members.get(submit.member());
    if (member != null) {
      member.tasks.onFail(submit.id());
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
