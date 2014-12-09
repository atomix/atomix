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

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Internal cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedCluster implements ManagedCluster {
  private final Cluster parent;
  private final CopycatStateContext state;
  private final Router router;
  private LocalMember localMember;
  private Map<String, Member> remoteMembers = new HashMap<>();
  private final ClusterElection election;

  public CoordinatedCluster(Cluster parent, CopycatStateContext state, Router router) {
    this.parent = parent;
    this.state = state;
    this.router = router;
    this.localMember = new CoordinatedLocalMember(parent.localMember(), state.executor());
    for (String uri : state.getRemoteMembers()) {
      this.remoteMembers.put(uri, new CoordinatedMember(parent.member(uri), state.executor()));
    }
    this.election = new ClusterElection(this, state);
  }

  public CopycatStateContext state() {
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
        remoteMembers.put(uri, new CoordinatedMember(parent.member(uri), state.executor()));
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
