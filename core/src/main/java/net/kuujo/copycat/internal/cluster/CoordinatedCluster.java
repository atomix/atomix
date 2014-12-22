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

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.internal.CopycatStateContext;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;

/**
 * Stateful cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedCluster extends CoordinatedClusterManager implements Cluster {
  private final CoordinatedClusterElection election;
  private final Router router;
  private final CopycatStateContext context;

  public CoordinatedCluster(int id, ClusterCoordinator coordinator, CopycatStateContext context, Router router, ExecutionContext executor) {
    super(id, coordinator, executor);
    this.election = new CoordinatedClusterElection(this, context);
    this.router = router;
    this.context = context;
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
  public CompletableFuture<Void> open() {
    router.createRoutes(this, context);
    election.open();
    return super.open();
  }

  @Override
  public CompletableFuture<Void> close() {
    router.destroyRoutes(this, context);
    election.close();
    return super.close();
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getCanonicalName(), members());
  }

}
