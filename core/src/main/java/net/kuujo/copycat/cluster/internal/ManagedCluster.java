/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License.
 */
package net.kuujo.copycat.cluster.internal;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.*;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.raft.protocol.Request;
import net.kuujo.copycat.raft.protocol.Response;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.Futures;
import net.kuujo.copycat.util.internal.Assert;
import net.kuujo.copycat.util.serializer.KryoSerializer;
import net.kuujo.copycat.util.serializer.Serializer;

import java.nio.ByteBuffer;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Default cluster implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ManagedCluster implements Cluster, Managed<Cluster>, Observer {
  private final RaftContext raft;
  private final ManagedMembers members;
  private final Set<EventListener<ElectionEvent>> listeners = new CopyOnWriteArraySet<>();
  private final Serializer serializer = new KryoSerializer();
  private String lastLeader;
  private long lastTerm;

  public ManagedCluster(ClusterConfig config, ResourceContext context) {
    this.raft = Assert.isNotNull(context, "context").raft();
    this.members = new ManagedMembers(config, context);
  }

  @Override
  public void update(Observable o, Object arg) {
    RaftContext raft = (RaftContext) o;
    if (raft.getTerm() > lastTerm && (lastLeader == null || (raft.getLeader() != null && !lastLeader.equals(raft.getLeader())))) {
      lastTerm = raft.getTerm();
      lastLeader = raft.getLeader();
      listeners.forEach(l -> l.accept(new ElectionEvent(ElectionEvent.Type.COMPLETE, lastTerm, member(lastLeader))));
    }
  }

  @Override
  public Member leader() {
    Assert.state(isOpen(), "cluster not open");
    return members.members.get(raft.getLeader());
  }

  @Override
  public long term() {
    Assert.state(isOpen(), "cluster not open");
    return raft.getTerm();
  }

  @Override
  public ManagedLocalMember member() {
    Assert.state(isOpen(), "cluster not open");
    return (ManagedLocalMember) members.members.get(raft.getLocalMember().id());
  }

  @Override
  public Member member(String id) {
    Assert.state(isOpen(), "cluster not open");
    return members.members.get(id);
  }

  @Override
  public Members members() {
    Assert.state(isOpen(), "cluster not open");
    return members;
  }

  @Override
  public <T> Cluster broadcast(String topic, T message) {
    Assert.state(isOpen(), "cluster not open");
    members.forEach(m -> {
      if (m instanceof ManagedRemoteMember) {
        m.send(topic, message);
      }
    });
    return this;
  }

  @Override
  public Cluster addMembershipListener(EventListener<MembershipEvent> listener) {
    members.addListener(listener);
    return this;
  }

  @Override
  public Cluster removeMembershipListener(EventListener<MembershipEvent> listener) {
    members.removeListener(listener);
    return this;
  }

  @Override
  public Cluster addElectionListener(EventListener<ElectionEvent> listener) {
    listeners.add(listener);
    return this;
  }

  @Override
  public Cluster removeElectionListener(EventListener<ElectionEvent> listener) {
    listeners.remove(listener);
    return this;
  }

  /**
   * Registers handlers for the context.
   */
  private void registerHandlers() {
    member().registerInternalHandler(InternalTopics.JOIN, wrapInboundRequest(raft::join));
    member().registerInternalHandler(InternalTopics.PROMOTE, wrapInboundRequest(raft::promote));
    member().registerInternalHandler(InternalTopics.LEAVE, wrapInboundRequest(raft::leave));
    member().registerInternalHandler(InternalTopics.SYNC, wrapInboundRequest(raft::sync));
    member().registerInternalHandler(InternalTopics.POLL, wrapInboundRequest(raft::poll));
    member().registerInternalHandler(InternalTopics.VOTE, wrapInboundRequest(raft::vote));
    member().registerInternalHandler(InternalTopics.APPEND, wrapInboundRequest(raft::append));
    member().registerInternalHandler(InternalTopics.QUERY, wrapInboundRequest(raft::query));
    member().registerInternalHandler(InternalTopics.COMMAND, wrapInboundRequest(raft::command));
    raft.joinHandler(request -> wrapOutboundRequest(InternalTopics.JOIN, request));
    raft.promoteHandler(request -> wrapOutboundRequest(InternalTopics.PROMOTE, request));
    raft.leaveHandler(request -> wrapOutboundRequest(InternalTopics.LEAVE, request));
    raft.syncHandler(request -> wrapOutboundRequest(InternalTopics.SYNC, request));
    raft.pollHandler(request -> wrapOutboundRequest(InternalTopics.POLL, request));
    raft.voteHandler(request -> wrapOutboundRequest(InternalTopics.VOTE, request));
    raft.appendHandler(request -> wrapOutboundRequest(InternalTopics.APPEND, request));
    raft.queryHandler(request -> wrapOutboundRequest(InternalTopics.QUERY, request));
    raft.commandHandler(request -> wrapOutboundRequest(InternalTopics.COMMAND, request));
  }

  /**
   * Unregisters handlers for the context.
   */
  private void unregisterHandlers() {
    member().unregisterInternalHandler(InternalTopics.JOIN);
    member().unregisterInternalHandler(InternalTopics.PROMOTE);
    member().unregisterInternalHandler(InternalTopics.LEAVE);
    member().unregisterInternalHandler(InternalTopics.SYNC);
    member().unregisterInternalHandler(InternalTopics.POLL);
    member().unregisterInternalHandler(InternalTopics.VOTE);
    member().unregisterInternalHandler(InternalTopics.APPEND);
    member().unregisterInternalHandler(InternalTopics.QUERY);
    member().unregisterInternalHandler(InternalTopics.COMMAND);
    raft.joinHandler(null);
    raft.promoteHandler(null);
    raft.leaveHandler(null);
    raft.syncHandler(null);
    raft.pollHandler(null);
    raft.voteHandler(null);
    raft.appendHandler(null);
    raft.queryHandler(null);
    raft.commandHandler(null);
  }

  /**
   * Wraps an inbound context call.
   */
  private <T, U> MessageHandler<ByteBuffer, ByteBuffer> wrapInboundRequest(MessageHandler<T, U> handler) {
    return message -> CompletableFuture.completedFuture(null)
        .thenCompose(v -> handler.apply(serializer.readObject(message)))
        .thenApply(serializer::writeObject);
  }

  /**
   * Wraps an outbound context call.
   */
  private <T extends Request, U extends Response> CompletableFuture<U> wrapOutboundRequest(String topic, T request) {
    ManagedMember<?> member = members.members.get(request.id());
    if (member != null) {
      return member.sendInternal(topic, serializer.writeObject(request)).thenApply(serializer::readObject);
    }
    return Futures.exceptionalFuture(new ClusterException("Unknown member URI"));
  }

  @Override
  public CompletableFuture<Cluster> open() {
    raft.addObserver(this);
    return members.open()
      .thenRun(this::registerHandlers)
      .thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return members.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    raft.deleteObserver(this);
    unregisterHandlers();
    return members.close();
  }

  @Override
  public boolean isClosed() {
    return members.isClosed();
  }

}
