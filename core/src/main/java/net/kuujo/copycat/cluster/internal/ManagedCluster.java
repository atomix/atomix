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
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.raft.protocol.Request;
import net.kuujo.copycat.raft.protocol.Response;
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
import java.util.concurrent.Executor;

/**
 * Default cluster implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ManagedCluster implements Cluster, Managed<Cluster>, Observer {
  private final RaftContext context;
  private final ManagedMembers members;
  private final Set<EventListener<ElectionEvent>> listeners = new CopyOnWriteArraySet<>();
  private final Serializer serializer = new KryoSerializer();
  private String lastLeader;
  private long lastTerm;

  public ManagedCluster(Protocol protocol, RaftContext context, Serializer serializer, Executor executor) {
    this.context = Assert.isNotNull(context, "context");
    this.members = new ManagedMembers(protocol, context, serializer, executor);
  }

  @Override
  public void update(Observable o, Object arg) {
    RaftContext context = (RaftContext) o;
    if (context.getTerm() > lastTerm && (lastLeader == null || (context.getLeader() != null && !lastLeader.equals(context.getLeader())))) {
      lastTerm = context.getTerm();
      lastLeader = context.getLeader();
      listeners.forEach(l -> l.accept(new ElectionEvent(ElectionEvent.Type.COMPLETE, lastTerm, member(lastLeader))));
    }
  }

  @Override
  public Member leader() {
    return members.members.get(context.getLeader());
  }

  @Override
  public long term() {
    return context.getTerm();
  }

  @Override
  public ManagedLocalMember member() {
    return (ManagedLocalMember) members.members.get(context.getLocalMember().uri());
  }

  @Override
  public Member member(String uri) {
    return members.members.get(uri);
  }

  @Override
  public Members members() {
    return members;
  }

  @Override
  public <T> Cluster broadcast(String topic, T message) {
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
    member().registerInternalHandler(InternalTopics.JOIN, wrapInboundRequest(context::join));
    member().registerInternalHandler(InternalTopics.PROMOTE, wrapInboundRequest(context::promote));
    member().registerInternalHandler(InternalTopics.LEAVE, wrapInboundRequest(context::leave));
    member().registerInternalHandler(InternalTopics.SYNC, wrapInboundRequest(context::sync));
    member().registerInternalHandler(InternalTopics.POLL, wrapInboundRequest(context::poll));
    member().registerInternalHandler(InternalTopics.VOTE, wrapInboundRequest(context::vote));
    member().registerInternalHandler(InternalTopics.APPEND, wrapInboundRequest(context::append));
    member().registerInternalHandler(InternalTopics.QUERY, wrapInboundRequest(context::query));
    member().registerInternalHandler(InternalTopics.COMMAND, wrapInboundRequest(context::command));
    context.joinHandler(request -> wrapOutboundRequest(InternalTopics.JOIN, request));
    context.promoteHandler(request -> wrapOutboundRequest(InternalTopics.PROMOTE, request));
    context.leaveHandler(request -> wrapOutboundRequest(InternalTopics.LEAVE, request));
    context.syncHandler(request -> wrapOutboundRequest(InternalTopics.SYNC, request));
    context.pollHandler(request -> wrapOutboundRequest(InternalTopics.POLL, request));
    context.voteHandler(request -> wrapOutboundRequest(InternalTopics.VOTE, request));
    context.appendHandler(request -> wrapOutboundRequest(InternalTopics.APPEND, request));
    context.queryHandler(request -> wrapOutboundRequest(InternalTopics.QUERY, request));
    context.commandHandler(request -> wrapOutboundRequest(InternalTopics.COMMAND, request));
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
    context.joinHandler(null);
    context.promoteHandler(null);
    context.leaveHandler(null);
    context.syncHandler(null);
    context.pollHandler(null);
    context.voteHandler(null);
    context.appendHandler(null);
    context.queryHandler(null);
    context.commandHandler(null);
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
    ManagedMember<?> member = members.members.get(request.uri());
    if (member != null) {
      return member.sendInternal(topic, serializer.writeObject(request)).thenApply(serializer::readObject);
    }
    return Futures.exceptionalFuture(new ClusterException("Unknown member URI"));
  }

  @Override
  public CompletableFuture<Cluster> open() {
    context.addObserver(this);
    registerHandlers();
    return members.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return members.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    context.deleteObserver(this);
    unregisterHandlers();
    return members.close();
  }

  @Override
  public boolean isClosed() {
    return members.isClosed();
  }

}
