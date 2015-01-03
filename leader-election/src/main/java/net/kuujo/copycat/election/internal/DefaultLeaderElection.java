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

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.ResourceContext;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.election.ElectionEvent;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.internal.AbstractResource;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Default leader election implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultLeaderElection extends AbstractResource<LeaderElection> implements LeaderElection {
  private final Executor executor;
  private Consumer<Member> handler;
  private final EventListener<ElectionEvent> electionListener;

  public DefaultLeaderElection(ResourceContext context) {
    super(context);
    this.executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-election-" + context.name() + "-%d"));
    this.electionListener = result -> {
      if (handler != null) {
        executor.execute(() -> handler.accept(result.winner()));
      }
    };
  }

  @Override
  public LeaderElection handler(Consumer<Member> handler) {
    this.handler = handler;
    return this;
  }

  @Override
  public CompletableFuture<LeaderElection> open() {
    return super.open().thenApply(result -> {
      context.partition(1).cluster().election().addListener(electionListener);
      return this;
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    context.partition(1).cluster().election().removeListener(electionListener);
    return super.close();
  }

}
