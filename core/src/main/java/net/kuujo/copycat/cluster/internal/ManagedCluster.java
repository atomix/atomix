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
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.raft.RaftMember;
import net.kuujo.copycat.raft.protocol.Request;
import net.kuujo.copycat.raft.protocol.Response;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.Futures;

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
  private final CopycatSerializer serializer = new CopycatSerializer();
  private int lastLeader;
  private long lastTerm;

  public ManagedCluster(ClusterConfig config, ResourceContext context) {
    if (context == null)
      throw new NullPointerException("context cannot be null");
    this.raft = context.raft();
    this.members = new ManagedMembers(config, context);
  }

  @Override
  public void update(Observable o, Object arg) {
    RaftContext raft = (RaftContext) o;
    if (raft.getTerm() > lastTerm && (lastLeader == 0 || (raft.getLeader() != 0 && lastLeader != raft.getLeader()))) {
      lastTerm = raft.getTerm();
      lastLeader = raft.getLeader();
      listeners.forEach(l -> l.accept(new ElectionEvent(ElectionEvent.Type.COMPLETE, lastTerm, member(lastLeader))));
    }
  }

  /**
   * Checks that the cluster is open.
   */
  private void checkOpen() {
    if (!isOpen())
      throw new IllegalStateException("cluster not open");
  }

  @Override
  public Member leader() {
    checkOpen();
    return members.members.get(raft.getLeader());
  }

  @Override
  public long term() {
    checkOpen();
    return raft.getTerm();
  }

  @Override
  public ManagedLocalMember member() {
    checkOpen();
    return (ManagedLocalMember) members.members.get(raft.getLocalMember().id());
  }

  @Override
  public Member member(int id) {
    checkOpen();
    RaftMember member = raft.getMember(id);
    String address = member.get("address");
    return address != null ? member(address) : null;
  }

  @Override
  public Member member(String address) {
    checkOpen();
    return members.members.get(address);
  }

  @Override
  public Members members() {
    checkOpen();
    return members;
  }

  @Override
  public <T> Cluster broadcast(String topic, T message) {
    checkOpen();
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
    member().registerInternalHandler(InternalTopics.READ, wrapInboundRequest(raft::read));
    member().registerInternalHandler(InternalTopics.WRITE, wrapInboundRequest(raft::write));
    member().registerInternalHandler(InternalTopics.DELETE, wrapInboundRequest(raft::delete));
    raft.joinHandler(request -> wrapOutboundRequest(InternalTopics.JOIN, request));
    raft.promoteHandler(request -> wrapOutboundRequest(InternalTopics.PROMOTE, request));
    raft.leaveHandler(request -> wrapOutboundRequest(InternalTopics.LEAVE, request));
    raft.syncHandler(request -> wrapOutboundRequest(InternalTopics.SYNC, request));
    raft.pollHandler(request -> wrapOutboundRequest(InternalTopics.POLL, request));
    raft.voteHandler(request -> wrapOutboundRequest(InternalTopics.VOTE, request));
    raft.appendHandler(request -> wrapOutboundRequest(InternalTopics.APPEND, request));
    raft.readHandler(request -> wrapOutboundRequest(InternalTopics.READ, request));
    raft.writeHandler(request -> wrapOutboundRequest(InternalTopics.WRITE, request));
    raft.deleteHandler(request -> wrapOutboundRequest(InternalTopics.DELETE, request));
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
    member().unregisterInternalHandler(InternalTopics.READ);
    member().unregisterInternalHandler(InternalTopics.WRITE);
    member().unregisterInternalHandler(InternalTopics.DELETE);
    raft.joinHandler(null);
    raft.promoteHandler(null);
    raft.leaveHandler(null);
    raft.syncHandler(null);
    raft.pollHandler(null);
    raft.voteHandler(null);
    raft.appendHandler(null);
    raft.readHandler(null);
    raft.writeHandler(null);
    raft.deleteHandler(null);
  }

  /**
   * Wraps an inbound context call.
   */
  private <T, U> MessageHandler<Buffer, Buffer> wrapInboundRequest(MessageHandler<T, U> handler) {
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
