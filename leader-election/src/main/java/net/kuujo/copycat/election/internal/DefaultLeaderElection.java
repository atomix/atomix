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
package net.kuujo.copycat.election.internal;

import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.election.ElectionResult;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.internal.AbstractCopycatResource;
import net.kuujo.copycat.log.ZeroRetentionPolicy;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Default leader election implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultLeaderElection extends AbstractCopycatResource<LeaderElection> implements LeaderElection {
  private Consumer<Member> handler;
  private final Consumer<ElectionResult> electionListener = result -> {
    if (handler != null) {
      executor.execute(() -> handler.accept(result.winner()));
    }
  };

  public DefaultLeaderElection(String name, CopycatContext context, ClusterCoordinator coordinator, ExecutionContext executor) {
    super(name, context, coordinator, executor);
    context.log().config()
      .withFlushOnWrite(true)
      .withRetentionPolicy(new ZeroRetentionPolicy());
  }

  @Override
  public LeaderElection handler(Consumer<Member> handler) {
    this.handler = handler;
    return this;
  }

  @Override
  public CompletableFuture<Void> open() {
    return super.open().thenAccept(result -> {
      context.cluster().election().addListener(electionListener);
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    context.cluster().election().removeListener(electionListener);
    return super.close();
  }

}
