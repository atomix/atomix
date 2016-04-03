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

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.copycat.Command;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.group.DistributedGroup;
import io.atomix.group.LocalMember;
import io.atomix.group.Member;
import io.atomix.group.election.Election;
import io.atomix.group.election.internal.GroupElection;
import io.atomix.group.messaging.MessageClient;
import io.atomix.group.messaging.internal.GroupMessage;
import io.atomix.group.messaging.internal.GroupMessageClient;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.ResourceType;
import io.atomix.resource.WriteConsistency;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
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
  private final Listeners<Member> joinListeners = new Listeners<>();
  private final Listeners<Member> leaveListeners = new Listeners<>();
  private final Set<String> joining = new CopyOnWriteArraySet<>();
  private final DistributedGroup.Options options;
  private final GroupElection election = new GroupElection(this);
  private final GroupMessageClient messages;
  private final Map<String, AbstractGroupMember> members = new ConcurrentHashMap<>();
  private final GroupSubmitter submitter = new GroupSubmitter() {
    @Override
    public <T extends Command<U>, U> CompletableFuture<U> submit(T command) {
      return MembershipGroup.this.submit(command, WriteConsistency.SEQUENTIAL_EVENT);
    }
  };

  public MembershipGroup(CopycatClient client, Properties options) {
    super(client, new ResourceType(DistributedGroup.class), options);
    this.options = new DistributedGroup.Options(Assert.notNull(options, "options"));
    this.messages = new GroupMessageClient(submitter);
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
  public MessageClient messages() {
    return messages;
  }

  @Override
  public Member member(String memberId) {
    return members.get(memberId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Member> members() {
    return (Collection) members.values();
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
    return submit(new GroupCommands.Join(memberId, persistent)).whenComplete((result, error) -> {
      if (error != null) {
        joining.remove(memberId);
      }
    }).thenApply(info -> {
      LocalGroupMember member = (LocalGroupMember) members.get(info.memberId());
      if (member == null) {
        member = new LocalGroupMember(info, this, submitter);
        members.put(info.memberId(), member);
      }
      return member;
    });
  }

  @Override
  public Listener<Member> onJoin(Consumer<Member> listener) {
    return joinListeners.add(listener);
  }

  @Override
  public CompletableFuture<Void> remove(String memberId) {
    return submit(new GroupCommands.Leave(memberId)).thenRun(() -> {
      members.remove(memberId);
    });
  }

  @Override
  public Listener<Member> onLeave(Consumer<Member> listener) {
    return leaveListeners.add(listener);
  }

  @Override
  public CompletableFuture<DistributedGroup> open() {
    return client.connect().thenApply(result -> {
      client.onEvent("join", this::onJoinEvent);
      client.onEvent("leave", this::onLeaveEvent);
      client.onEvent("message", this::onMessageEvent);
      client.onEvent("ack", this::onReplyEvent);
      client.onEvent("fail", this::onFailEvent);
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
    return submit(new GroupCommands.Listen()).thenAccept(members -> {
      for (GroupMemberInfo info : members) {
        AbstractGroupMember member = this.members.get(info.memberId());
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
    AbstractGroupMember member;
    if (joining.remove(info.memberId())) {
      member = new LocalGroupMember(info, this, submitter);
      members.put(info.memberId(), member);

      // Trigger join listeners.
      joinListeners.accept(member);
    } else {
      member = members.get(info.memberId());
      if (member == null) {
        member = new GroupMember(info, this, submitter);
        members.put(info.memberId(), member);

        // Trigger join listeners.
        joinListeners.accept(member);
      }
    }
  }

  /**
   * Handles a leave event received from the cluster.
   */
  private void onLeaveEvent(String memberId) {
    Member member = members.remove(memberId);
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
    AbstractGroupMember localMember = members.get(message.member());
    if (localMember != null && localMember instanceof LocalMember) {
      ((LocalGroupMember) localMember).messages().consumer(message.queue()).onTask(message);
    }
  }

  /**
   * Handles a reply event received from the cluster.
   */
  private void onReplyEvent(GroupCommands.Reply reply) {
    if (reply.member() != null) {
      AbstractGroupMember member = members.get(reply.member());
      if (member != null) {
        member.messages().producer(reply.queue()).onReply(reply.id(), reply.message());
      }
    } else {
      messages.producer(reply.queue()).onReply(reply.id(), reply.message());
    }
  }

  /**
   * Handles a fail event received from the cluster.
   */
  private void onFailEvent(GroupCommands.Send send) {
    if (send.member() != null) {
      AbstractGroupMember member = members.get(send.member());
      if (member != null) {
        member.messages().producer(send.queue()).onFail(send.id());
      }
    } else {
      messages.producer(send.queue()).onFail(send.id());
    }
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
