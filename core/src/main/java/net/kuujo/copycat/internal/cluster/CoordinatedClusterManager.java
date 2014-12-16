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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ClusterManager;
import net.kuujo.copycat.cluster.LocalMember;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.coordinator.MemberCoordinator;
import net.kuujo.copycat.spi.ExecutionContext;

/**
 * Internal cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedClusterManager implements ClusterManager {
  private CoordinatedLocalMember localMember;
  private final Map<String, CoordinatedMember> remoteMembers = new HashMap<>();

  public CoordinatedClusterManager(int id, ClusterCoordinator coordinator, ExecutionContext executor) {
    this.localMember = new CoordinatedLocalMember(id, coordinator.localMember(), executor);
    for (MemberCoordinator member : coordinator.remoteMembers()) {
      this.remoteMembers.put(member.uri(), new CoordinatedMember(id, member, executor));
    }
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
  public Collection<Member> remoteMembers() {
    return Collections.unmodifiableCollection(remoteMembers.values());
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
