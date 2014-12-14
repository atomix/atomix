/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.LocalMember;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.internal.CopycatStateContext;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Internal cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedCluster implements Cluster {
  private final int id;
  private final ClusterCoordinator coordinator;
  private CopycatStateContext context;
  private final ExecutionContext executor;
  private final Router router;
  private CoordinatedLocalMember localMember;
  private Map<String, CoordinatedMember> remoteMembers = new HashMap<>();
  private final ClusterElection election;

  public CoordinatedCluster(int id, ClusterCoordinator coordinator, CopycatStateContext context, Router router, ExecutionContext executor) {
    this.id = id;
    this.coordinator = coordinator;
    this.router = router;
    this.executor = executor;
    this.localMember = new CoordinatedLocalMember(id, coordinator.localMember(), executor);
    for (String uri : context.getRemoteMembers()) {
      this.remoteMembers.put(uri, new CoordinatedMember(id, coordinator.member(uri), executor));
    }
    this.election = new ClusterElection(this, context);
  }

  @Override
  public Member leader() {
    return context.getLeader() != null ? member(context.getLeader()) : null;
  }

  @Override
  public long term() {
    return context.getTerm();
  }

  @Override
  public Election election() {
    return election;
  }

  @Override
  public Set<Member> members() {
    Set<Member> members = new HashSet<>(remoteMembers.size() + 1);
    members.add(localMember);
    return members;
  }

  @Override
  public Member member(String uri) {
    Member member = remoteMembers.get(uri);
    if (member != null) {
      return member;
    } else if (localMember.uri().equals(uri)) {
      return localMember;
    }
    return null;
  }

  @Override
  public LocalMember localMember() {
    return localMember;
  }

  @Override
  public Set<Member> remoteMembers() {
    return new HashSet<>(remoteMembers.values());
  }

  @Override
  public Member remoteMember(String uri) {
    return remoteMembers.get(uri);
  }

  @Override
  public CompletableFuture<Cluster> configure(ClusterConfig config) {
    CompletableFuture<Cluster> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("Configuration changes not supported"));
    return future;
  }

  @Override
  public CompletableFuture<Void> open() {
    localMember.open();
    router.createRoutes(this, context);
    election.open();
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    localMember.close();
    router.destroyRoutes(this, context);
    election.close();
    return CompletableFuture.completedFuture(null);
  }

}
