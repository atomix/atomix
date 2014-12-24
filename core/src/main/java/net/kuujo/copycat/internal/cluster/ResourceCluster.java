/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.LocalMember;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

/**
 * Resource cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceCluster implements Cluster {
  private final CopycatContext context;
  private final ExecutionContext executor;

  public ResourceCluster(CopycatContext context, ExecutionContext executor) {
    this.context = context;
    this.executor = executor;
  }

  @Override
  public synchronized Member leader() {
    Member leader = context.cluster().leader();
    return leader != null ? leader instanceof LocalMember ? new LocalResourceMember((LocalMember) leader, context, executor) : new ResourceMember(leader, context, executor) : null;
  }

  @Override
  public synchronized long term() {
    return context.cluster().term();
  }

  @Override
  public synchronized Election election() {
    return new ResourceClusterElection(this, context.cluster().election(), executor);
  }

  @Override
  public synchronized Member member(String uri) {
    Member member = context.cluster().member(uri);
    return member != null ? member instanceof LocalMember ? new LocalResourceMember((LocalMember) member, context, executor) : new ResourceMember(member, context, executor) : null;
  }

  @Override
  public synchronized LocalMember member() {
    LocalMember member = context.cluster().member();
    return member != null ? new LocalResourceMember(member, context, executor) : null;
  }

  @Override
  public synchronized Collection<Member> members() {
    Collection<Member> members = new HashSet<>();
    for (Member member : context.cluster().members()) {
      members.add(new ResourceMember(member, context, executor));
    }
    return members;
  }

  @Override
  public CompletableFuture<Void> open() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getCanonicalName(), members());
  }

}
