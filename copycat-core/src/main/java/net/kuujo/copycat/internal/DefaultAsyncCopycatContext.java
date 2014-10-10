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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.AsyncCopycatContext;
import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.spi.protocol.AsyncProtocol;

import java.util.concurrent.CompletableFuture;

/**
 * Default asynchronous Copycat context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncCopycatContext extends AbstractCopycatContext implements AsyncCopycatContext {

  <M extends Member> DefaultAsyncCopycatContext(StateMachine stateMachine, Log log, Cluster<M> cluster, AsyncProtocol<M> protocol, CopycatConfig config) {
    super(new StateContext(stateMachine, log, cluster, protocol, config), cluster, config);
  }

  @Override
  public CompletableFuture<Void> start() {
    return state.start();
  }

  @Override
  public CompletableFuture<Void> stop() {
    return state.stop();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R> CompletableFuture<R> submit(final String operation, final Object... args) {
    return state.submit(Assert.isNotNull(operation, "operation cannot be null"), args);
  }

  @Override
  public String toString() {
    return String.format("%s[state=%s]", getClass().getSimpleName(), state.state());
  }

}
