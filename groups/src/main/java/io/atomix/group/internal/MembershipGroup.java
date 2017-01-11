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
package io.atomix.group.internal;

import io.atomix.catalyst.concurrent.Futures;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.Listeners;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.group.DistributedGroup;
import io.atomix.group.GroupMember;
import io.atomix.group.LocalMember;
import io.atomix.group.election.Election;
import io.atomix.group.election.internal.GroupElection;
import io.atomix.group.messaging.MessageClient;
import io.atomix.group.messaging.internal.GroupMessage;
import io.atomix.group.messaging.internal.GroupMessageClient;
import io.atomix.group.messaging.internal.MessageConsumerService;
import io.atomix.group.messaging.internal.MessageProducerService;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.ResourceType;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Base {@link DistributedGroup} implementation which manages a membership set for the group.
 * <p>
 * The membership group is the base {@link DistributedGroup} type which is created when a new group
 * is created via the Atomix API.
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("foo").get();
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MembershipGroup extends AbstractResource<DistributedGroup> implements DistributedGroup {
  private final Listeners<GroupMember> joinListeners = new Listeners<>();
  private final Listeners<GroupMember> leaveListeners = new Listeners<>();
  private final GroupElection election = new GroupElection(this);
  private final GroupMessageClient messages;
  private final DistributedGroup.Options options;
  private final Map<String, AbstractGroupMember> members = new ConcurrentHashMap<>();
  private final MessageProducerService producerService;
  private final MessageConsumerService consumerService;
  private final Map<String, GroupCommands.Join> localJoins = new ConcurrentHashMap<>();

  public MembershipGroup(CopycatClient client, Properties options) {
    super(client, new ResourceType(DistributedGroup.class), options);
    this.producerService = new MessageProducerService(this.client);
    this.consumerService = new MessageConsumerService(this.client);
    this.messages = new GroupMessageClient(producerService);
    this.options = new DistributedGroup.Options(options);
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
  public Election election() {
    return election;
  }

  @Override
  public MessageClient messaging() {
    return messages;
  }

  @Override
  public GroupMember member(String memberId) {
    return members.get(memberId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<GroupMember> members() {
    return (Collection) members.values();
  }

  @Override
  public CompletableFuture<LocalMember> join() {
    return join(UUID.randomUUID().toString(), false, null);
  }

  @Override
  public CompletableFuture<LocalMember> join(String memberId) {
    return join(memberId, true, null);
  }

  @Override
  public CompletableFuture<LocalMember> join(Object metadata) {
    return join(UUID.randomUUID().toString(), false, metadata);
  }

  @Override
  public CompletableFuture<LocalMember> join(String memberId, Object metadata) {
    return join(memberId == null ? UUID.randomUUID().toString() : memberId, memberId != null, metadata);
  }

  /**
   * Joins the group.
   *
   * @param memberId The member ID with which to join the group.
   * @param persistent Indicates whether the member ID is persistent.
   * @return A completable future to be completed once the member has joined the group.
   */
  private CompletableFuture<LocalMember> join(String memberId, boolean persistent, Object metadata) {
    // When joining a group, the join request is guaranteed to complete prior to the join
    // event being received.
    final GroupCommands.Join cmd = new GroupCommands.Join(memberId, persistent, metadata);
    return client.submit(cmd).thenApply(info -> {
      AbstractGroupMember member = members.get(info.memberId());
      if (member == null || !(member instanceof LocalGroupMember)) {
        member = new LocalGroupMember(info, this, producerService, consumerService);
        localJoins.put(memberId, cmd);
        members.put(info.memberId(), member);
      }
      return (LocalGroupMember) member;
    });
  }

  @Override
  public Listener<GroupMember> onJoin(Consumer<GroupMember> listener) {
    return joinListeners.add(listener);
  }

  @Override
  public CompletableFuture<Void> remove(String memberId) {
    return client.submit(new GroupCommands.Leave(memberId)).thenRun(() -> {
      localJoins.remove(memberId);
      GroupMember member = members.remove(memberId);
      if (member != null) {
        leaveListeners.accept(member);
      }
    });
  }

  @Override
  public Listener<GroupMember> onLeave(Consumer<GroupMember> listener) {
    return leaveListeners.add(listener);
  }

  @Override
  public CompletableFuture<DistributedGroup> open() {
    return super.open().thenApply(result -> {
      client.onEvent("join", this::onJoinEvent);
      client.onEvent("leave", this::onLeaveEvent);
      client.onEvent("alive", this::onAliveEvent);
      client.onEvent("dead", this::onDeadEvent);
      client.onEvent("message", this::onMessageEvent);
      client.onEvent("ack", this::onAckEvent);
      client.onEvent("term", this::onTermEvent);
      client.onEvent("elect", this::onElectEvent);
      return result;
    }).thenCompose(v -> sync())
      .thenApply(v -> this);
  }

  @Override
  protected CompletableFuture<Void> recover(Integer attempt) {
    Boolean recover = Boolean.parseBoolean(options.getProperty("recover", "true"));
    if (!recover) {
      return Futures.completedFuture(null);
    }

    // When recovering the membership group, we need to ensure that all local non-persistent members are
    // removed from the group prior to fetching the group membership from the cluster again, and prior to
    // adding recovered members that the membership list is updated to ensure the list is consistent
    // when join event handlers are called.
    Map<String, GroupCommands.Join> joins = new HashMap<>(localJoins);
    return sync()
      .thenCompose(v -> {
        List<CompletableFuture> futures = new ArrayList<>(joins.size());
        for (GroupCommands.Join join : joins.values()) {
          if (!join.persist()) {
            futures.add(remove(join.member()));
          }
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
      })
      .thenCompose(v -> sync())
      .thenCompose(v -> {
        List<CompletableFuture> futures = new ArrayList<>(joins.size());
        for (GroupCommands.Join join : joins.values()) {
          if (join.persist()) {
            futures.add(join(join.member(), join.persist(), join.metadata()));
          } else {
            futures.add(join(join.metadata()));
          }
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
      });
  }

  /**
   * Synchronizes the membership group.
   */
  private CompletableFuture<Void> sync() {
    return client.submit(new GroupCommands.Listen()).thenAccept(status -> {
      for (GroupMemberInfo info : status.members()) {
        AbstractGroupMember member = this.members.get(info.memberId());
        if (member == null) {
          member = new RemoteGroupMember(info, this, producerService);
          this.members.put(member.id(), member);
        }
      }

      election.setTerm(status.term());
      if (status.leader() != null) {
        GroupMember leader = this.members.get(status.leader());
        if (leader != null) {
          election.setLeader(leader);
        }
      }
    });
  }

  /**
   * Handles a join event received from the cluster.
   */
  private void onJoinEvent(GroupMemberInfo info) {
    // If the join event was for a local member, the local member is guaranteed to have already
    // been created since responses will always be received before events. Therefore, if the member
    // is null we can create a remote member. If the member is a local member, only call join listeners.
    // Local member join listeners are called here to ensure they're called *after* the join future
    // completes as is guaranteed by the event framework.
    AbstractGroupMember member = members.get(info.memberId());
    if (member == null) {
      member = new RemoteGroupMember(info, this, producerService);
      members.put(info.memberId(), member);
      joinListeners.accept(member);
    } else {
      member.onStatusChange(GroupMember.Status.ALIVE);
      if (member instanceof LocalGroupMember) {
        joinListeners.accept(member);
      }
    }
  }

  /**
   * Handles a leave event received from the cluster.
   */
  private void onLeaveEvent(String memberId) {
    GroupMember member = members.remove(memberId);
    if (member != null) {
      // Trigger leave listeners.
      leaveListeners.accept(member);
    }
  }

  /**
   * Handles a member status change to ALIVE.
   */
  private void onAliveEvent(String memberId) {
    AbstractGroupMember member = members.get(memberId);
    if (member != null) {
      member.onStatusChange(GroupMember.Status.ALIVE);
    }
  }

  /**
   * Handles a member status change to DEAD.
   */
  private void onDeadEvent(String memberId) {
    AbstractGroupMember member = members.get(memberId);
    if (member != null) {
      member.onStatusChange(GroupMember.Status.DEAD);
    }
  }

  /**
   * Handles a message event received from the cluster.
   */
  @SuppressWarnings("unchecked")
  private void onMessageEvent(GroupMessage message) {
    consumerService.onMessage(message);
  }

  /**
   * Handles an ack event received from the cluster.
   */
  private void onAckEvent(GroupCommands.Ack ack) {
    producerService.onAck(ack);
  }

  /**
   * Handles a term change event.
   */
  private void onTermEvent(long term) {
    election.onTerm(term);
  }

  /**
   * Handles an elect event.
   */
  private void onElectEvent(String memberId) {
    AbstractGroupMember member = members.get(memberId);
    if (member != null) {
      election.onElection(member);
    }
  }

}
