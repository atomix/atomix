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
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.spi.protocol.AsyncProtocol;
import net.kuujo.copycat.spi.protocol.Protocol;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Synchronous Copycat replica.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Copycat extends AbstractCopycat {

  /**
   * Constructs a synchronous Copycat replica with a default configuration.
   *
   * @param stateMachine The Copycat state machine.
   * @param log The Copycat log.
   * @param cluster The Copycat cluster configuration.
   * @param protocol The synchronous protocol.
   * @param <M> The cluster member type.
   */
  public <M extends Member> Copycat(StateMachine stateMachine, Log log, Cluster<M> cluster, AsyncProtocol<M> protocol) {
    this(stateMachine, log, cluster, protocol, new CopycatConfig());
  }

  /**
   * Constructs a synchronous Copycat replica with a user-defined configuration.
   *
   * @param stateMachine The Copycat state machine.
   * @param log The Copycat log.
   * @param cluster The Copycat cluster configuration.
   * @param protocol The synchronous protocol.
   * @param config The replica configuration.
   * @param <M> The cluster member type.
   */
  public <M extends Member> Copycat(StateMachine stateMachine, Log log, Cluster<M> cluster, AsyncProtocol<M> protocol, CopycatConfig config) {
    super(new StateContext(stateMachine, log, cluster, protocol, config), cluster, config);
  }

  private Copycat(StateContext state, Cluster<?> cluster, CopycatConfig config) {
    super(state, cluster, config);
  }

  /**
   * Copycat builder.
   */
  public static class Builder extends AbstractCopycat.Builder<Copycat, Protocol<?>> {
    public Builder() {
      super((builder) -> new Copycat(new StateContext(builder.stateMachine, builder.log, builder.cluster, builder.protocol, builder.config), builder.cluster, builder.config));
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
   * Starts the replica.
   */
  public void start() {
    CountDownLatch latch = new CountDownLatch(1);
    state.start().thenRun(latch::countDown);
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new CopycatException(e);
    };
  }

  /**
   * Stops the replica.
   */
  public void stop() {
    CountDownLatch latch = new CountDownLatch(1);
    state.stop().thenRun(latch::countDown);
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new CopycatException(e);
    };
  }

  /**
   * Submits an operation to the cluster.
   *
   * @param operation The name of the operation to submit.
   * @param args An ordered list of operation arguments.
   * @return The operation result.
   * @throws NullPointerException if {@code operation} is null
   */
  @SuppressWarnings("unchecked")
  public <R> R submit(final String operation, final Object... args) {
    final CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<R> result = new AtomicReference<>();
    state.submit(Assert.isNotNull(operation, "operation cannot be null"), args).whenComplete(
        (r, error) -> {
          latch.countDown();
          result.set((R) r);
        });
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new CopycatException(e);
    }
    return result.get();
  }
}
