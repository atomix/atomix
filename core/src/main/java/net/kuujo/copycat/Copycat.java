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
 * Synchronous Copycat replica.<p>
 *
 * <b>Note that synchronous replicas are currently experimental</b>
 *
 * The Copycat replica is a fault-tolerant, replicated container for a {@link net.kuujo.copycat.StateMachine}.
 * Given a cluster of {@code Copycat} replicas, Copycat guarantees that commands and queries applied to the
 * state machine will be applied in the same order on all nodes (unless configuration specifies otherwise).<p>
 *
 * <pre>
 * {@code
 * // Create the state machine.
 * StateMachine stateMachine = new DataStore();
 *
 * // Create the log.
 * Log log = new MemoryMappedFileLog("data.log");
 *
 * // Create the cluster configuration.
 * TcpClusterConfig clusterConfig = new TcpClusterConfig()
 *   .withLocalMember(new TcpMember("localhost", 1234))
 *   .withRemoteMembers(new TcpMember("localhost", 2345), new TcpMember("localhost", 3456));
 * TcpCluster cluster = new TcpCluster(clusterConfig);
 *
 * // Create a TCP protocol.
 * BasicTcpProtocol protocol = new BasicTcpProtocol();
 *
 * // Create a synchronous Copycat instance.
 * Copycat copycat = new Copycat(stateMachine, log, cluster, protocol);
 *
 * // Start the Copycat instance.
 * copycat.start();
 *
 * copycat.submit("set", "foo", "Hello world!");
 * String result = copycat.submit("get", "foo");
 * assertEquals("Hello world!", result);
 * }
 * </pre>
 *
 * In order to provide this guarantee, state machines must be designed accordingly. State machines must be
 * deterministic, meaning given the same commands in the same order, the state machine will always create
 * the same state.<p>
 *
 * To create a state machine, simple implement the {@link StateMachine} interface.
 *
 * <pre>
 * {@code
 * public class DataStore implements StateMachine {
 *   private final Map<String, Object> data = new HashMap<>();
 *
 *   @Command
 *   public void set(String name, Object value) {
 *     data.put(name, value);
 *   }
 *
 *   @Query
 *   public void get(String name) {
 *     return data.get(name);
 *   }
 *
 * }
 * }
 * </pre><p>
 *
 * Copycat will wrap this state machine on any number of nodes and ensure commands submitted
 * to the cluster are applied to the state machine in the order in which they're received.
 * Copycat supports two different types of operations - {@link net.kuujo.copycat.Command}
 * and {@link net.kuujo.copycat.Query}. {@link net.kuujo.copycat.Command} operations are write
 * operations that modify the state machine's state. All commands submitted to the cluster
 * will go through the cluster leader to ensure log order. {@link net.kuujo.copycat.Query}
 * operations are read-only operations that do not modify the state machine's state. Copycat
 * can be optionally configured to allow read-only operations on follower nodes.<p>
 *
 * As mentioned, underlying each Copycat replica is a persistent {@link net.kuujo.copycat.log.Log}.
 * The log is a strongly ordered sequence of events which Copycat replicates between leader and
 * followers. Copycat provides several {@link net.kuujo.copycat.log.Log} implementations for
 * different use cases.<p>
 *
 * Copycat also provides extensible {@link net.kuujo.copycat.spi.protocol.Protocol} support.
 * The Copycat replication implementation is completely protocol agnostic, so users can use
 * Copycat provided protocols or custom protocols. Each {@link net.kuujo.copycat.Copycat} instance
 * is thus associated with a {@link net.kuujo.copycat.cluster.Cluster} and
 * {@link net.kuujo.copycat.spi.protocol.Protocol} which it uses for communication between replicas.
 * It is very important that all nodes within the same Copycat cluster use the same protocol for
 * obvious reasons.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Copycat extends Replica {

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
  public static class Builder extends Replica.Builder<Copycat, Protocol<?>> {
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
