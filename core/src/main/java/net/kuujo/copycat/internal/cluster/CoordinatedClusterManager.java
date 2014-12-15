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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ClusterManager;
import net.kuujo.copycat.cluster.LocalMember;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
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
public class CoordinatedClusterManager implements ClusterManager {
  private final int id;
  private final ClusterCoordinator coordinator;
  private CopycatStateContext context;
  private final ExecutionContext executor;
  private CoordinatedLocalMember localMember;
  private Map<String, CoordinatedMember> remoteMembers = new HashMap<>();

  public CoordinatedClusterManager(int id, ClusterCoordinator coordinator, ExecutionContext executor) {
    this.id = id;
    this.coordinator = coordinator;
    this.executor = executor;
    this.localMember = new CoordinatedLocalMember(id, coordinator.localMember(), executor);
    for (String uri : context.getRemoteMembers()) {
      this.remoteMembers.put(uri, new CoordinatedMember(id, coordinator.member(uri), executor));
    }
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
  public CompletableFuture<ClusterManager> configure(ClusterConfig config) {
    CompletableFuture<ClusterManager> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("Configuration changes not supported"));
    return future;
  }

  @Override
  public CompletableFuture<Void> open() {
    localMember.open();
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    localMember.close();
    return CompletableFuture.completedFuture(null);
  }

}
