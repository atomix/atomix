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

import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
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

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
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
  private final Map<String, AbstractGroupMember> members = new ConcurrentHashMap<>();
  private final MessageProducerService producerService;
  private final MessageConsumerService consumerService;

  public MembershipGroup(CopycatClient client, Properties options) {
    super(client, new ResourceType(DistributedGroup.class), options);
    this.producerService = new MessageProducerService(this.client);
    this.consumerService = new MessageConsumerService(this.client);
    this.messages = new GroupMessageClient(producerService);
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
  public CompletableFuture<LocalMember> join(String memberId, Object metadata) {
    return join(memberId == null ? UUID.randomUUID().toString() : memberId, false, metadata);
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
    return client.submit(new GroupCommands.Join(memberId, persistent, metadata)).thenApply(info -> {
      AbstractGroupMember member = members.get(info.memberId());
      if (member == null || !(member instanceof LocalGroupMember)) {
        member = new LocalGroupMember(info, this, producerService, consumerService);
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
      members.remove(memberId);
    });
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
      client.onEvent("message", this::onMessageEvent);
      client.onEvent("ack", this::onAckEvent);
      client.onEvent("term", this::onTermEvent);
      client.onEvent("elect", this::onElectEvent);
      return result;
    }).thenCompose(v -> sync())
      .thenApply(v -> this);
  }

  /**
   * Synchronizes the membership group.
   */
  private CompletableFuture<Void> sync() {
    return client.submit(new GroupCommands.Listen()).thenAccept(members -> {
      for (GroupMemberInfo info : members) {
        AbstractGroupMember member = this.members.get(info.memberId());
        if (member == null) {
          member = new RemoteGroupMember(info, this, producerService);
          this.members.put(member.id(), member);
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
    } else if (member instanceof LocalGroupMember) {
      joinListeners.accept(member);
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
