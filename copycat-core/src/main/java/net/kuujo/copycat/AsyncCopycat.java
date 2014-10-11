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
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.spi.protocol.Protocol;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous Copycat replica.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncCopycat extends AbstractCopycat {
  private AsyncCopycat(StateContext state, Cluster<?> cluster, CopycatConfig config) {
    super(state, cluster, config);
  }

  /**
   * Asynchronous Copycat builder.
   */
  public static class Builder extends AbstractCopycat.Builder<AsyncCopycat, Protocol<?>> {
    public Builder() {
      super((builder) -> new AsyncCopycat(new StateContext(builder.stateMachine, builder.log, builder.cluster, builder.protocol, builder.config), builder.cluster, builder.config));
    }
  }

  /**
   * Returns a new copycat builder.
   *
   * @return A new copycat builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Starts the context.
   *
   * @return A completable future to be completed once the context has started.
   */
  public CompletableFuture<Void> start() {
    return state.start();
  }

  /**
   * Stops the context.
   *
   * @return A completable future that will be completed when the context has started.
   */
  public CompletableFuture<Void> stop() {
    return state.stop();
  }

  /**
   * Submits a operation to the cluster.
   *
   * @param operation The name of the operation to submit.
   * @param args An ordered list of operation arguments.
   * @return A completable future to be completed once the result is received.
   * @throws NullPointerException if {@code operation} is null
   */
  public <R> CompletableFuture<R> submit(final String operation, final Object... args) {
    return state.submit(Assert.isNotNull(operation, "operation cannot be null"), args);
  }
}
