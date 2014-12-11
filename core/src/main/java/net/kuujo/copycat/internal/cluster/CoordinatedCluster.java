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

import net.kuujo.copycat.cluster.*;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.internal.CopycatStateContext;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Internal cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedCluster implements ManagedCluster {
  private final int id;
  private final InternalCluster parent;
  private CopycatStateContext state;
  private final ExecutionContext executor;
  private final Router router;
  private LocalMember localMember;
  private Map<String, Member> remoteMembers = new HashMap<>();
  private ClusterElection election;

  public CoordinatedCluster(int id, InternalCluster parent, Router router, ExecutionContext executor) {
    this.id = id;
    this.parent = parent;
    this.router = router;
    this.executor = executor;
    this.localMember = new CoordinatedLocalMember(id, parent.localMember(), executor);
    for (String uri : state.getRemoteMembers()) {
      this.remoteMembers.put(uri, new CoordinatedMember(id, parent.member(uri), executor));
    }
  }

  /**
   * Sets the cluster state.
   */
  public void setState(CopycatStateContext state) {
    this.state = state;
    this.election = new ClusterElection(this, state);
  }

  /**
   * Returns the cluster state.
   */
  public CopycatStateContext getState() {
    return state;
  }

  @Override
  public Member leader() {
    return state.getLeader() != null ? member(state.getLeader()) : null;
  }

  @Override
  public long term() {
    return state.getTerm();
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
    Iterator<Map.Entry<String, Member>> entryIterator = remoteMembers.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Map.Entry<String, Member> entry = entryIterator.next();
      if (!config.getMembers().contains(entry.getKey())) {
        entryIterator.remove();
      }
    }
    for (String uri : config.getRemoteMembers()) {
      if (!remoteMembers.containsKey(uri)) {
        remoteMembers.put(uri, new CoordinatedMember(id, parent.member(uri), executor));
      }
    }
    state.setMembers(config.getMembers());
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public CompletableFuture<Void> open() {
    router.createRoutes(this, state);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    router.destroyRoutes(this, state);
    return CompletableFuture.completedFuture(null);
  }

}
