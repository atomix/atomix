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
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.raft.RaftMember;
import net.kuujo.copycat.raft.protocol.ProtocolHandler;
import net.kuujo.copycat.raft.protocol.RaftProtocol;
import net.kuujo.copycat.raft.protocol.Request;
import net.kuujo.copycat.raft.protocol.Response;
import net.kuujo.copycat.resource.PartitionContext;
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
public class ManagedCluster implements Cluster, Managed<Void>, RaftProtocol, Observer {
  private final RaftContext raft;
  private final ManagedMembers members;
  private final Set<EventListener<ElectionEvent>> listeners = new CopyOnWriteArraySet<>();
  private final CopycatSerializer serializer = new CopycatSerializer();
  private int lastLeader;
  private long lastTerm;

  public ManagedCluster(ClusterConfig config, PartitionContext context) {
    if (context == null)
      throw new NullPointerException("context cannot be null");
    this.raft = context.getContext();
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

  @Override
  public CompletableFuture<Response> request(Request request, RaftMember raftMember, RaftContext raftContext) {
    ManagedMember<?> member = members.members.get(raftMember.id());
    if (member != null) {
      return member.sendInternal(serializer.writeObject(request)).thenApply(serializer::readObject);
    }
    return Futures.exceptionalFuture(new ClusterException("Unknown member"));
  }

  @Override
  public void requestHandler(ProtocolHandler<Request, Response> handler) {
    ((ManagedLocalMember) members.members.get(raft.getLocalMember().id())).registerInternalHandler(buffer -> {
      return handler.apply(serializer.readObject(buffer)).thenApply(serializer::writeObject);
    });
  }

  @Override
  public CompletableFuture<Void> open() {
    raft.addObserver(this);
    return members.open();
  }

  @Override
  public boolean isOpen() {
    return members.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    raft.deleteObserver(this);
    return members.close();
  }

  @Override
  public boolean isClosed() {
    return members.isClosed();
  }

}
