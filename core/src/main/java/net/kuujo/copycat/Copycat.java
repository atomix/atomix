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

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.event.Event;
import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.EventHandlerRegistry;
import net.kuujo.copycat.event.EventHandlers;
import net.kuujo.copycat.event.Events;
import net.kuujo.copycat.internal.event.DefaultEvents;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.spi.protocol.Protocol;

/**
 * Copycat is a fault-tolerant, replicated container for a {@link net.kuujo.copycat.StateMachine
 * state machine}. Given a cluster of {@code Copycat} replicas, Copycat guarantees that commands and
 * queries applied to the state machine will be applied in the same order on all replicas (unless
 * configuration specifies otherwise).
 * <p>
 *
 * <pre>
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
 * NettyTcpProtocol protocol = new NettyTcpProtocol();
 *
 * // Create Copycat instance.
 * Copycat copycat = new Copycat(stateMachine, log, cluster, protocol);
 *
 * // Start the Copycat instance.
 * copycat.start().thenRun(() -> {
 *   copycat.submit("set", "foo", "Hello world!").thenRun(() -> {
 *     copycat.submit("get", "foo").whenComplete((result, error) -> {
 *       assertEquals("Hello world!", result);
 *     });
 *   });
 * });
 * </pre>
 *
 * In order to provide this guarantee, state machines must be designed accordingly. State machines
 * must be deterministic, meaning given the same commands in the same order, the state machine will
 * always create the same state.
 * <p>
 *
 * To create a state machine, simple implement the {@link StateMachine} interface.
 *
 * <pre>
 * public class DataStore implements StateMachine {
 *   private final Map<String, Object> data = new HashMap<>();
 *
 *   &#064;Command
 *   public void set(String name, Object value) {
 *     data.put(name, value);
 *   }
 *
 *   &#064;Query
 *   public void get(String name) {
 *     return data.get(name);
 *   }
 * }
 * </pre>
 * <p>
 *
 * Copycat will wrap this state machine on any number of nodes and ensure commands submitted to the
 * cluster are applied to the state machine in the order in which they're received. Copycat supports
 * two different types of operations - {@link net.kuujo.copycat.Command commands} and
 * {@link net.kuujo.copycat.Query queries}. {@link net.kuujo.copycat.Command Command} operations are
 * write operations that modify the state machine's state. All commands submitted to the cluster
 * will go through the cluster leader to ensure log order. {@link net.kuujo.copycat.Query Query}
 * operations are read-only operations that do not modify the state machine's state. Copycat can be
 * optionally configured to allow read-only operations on follower nodes.
 * <p>
 *
 * As mentioned, underlying each Copycat replica is a persistent {@link net.kuujo.copycat.log.Log log}.
 * The log is a strongly ordered sequence of events which Copycat replicates between leader and
 * followers. Copycat provides several {@link net.kuujo.copycat.log.Log log} implementations for
 * different use cases.
 * <p>
 *
 * Copycat also provides extensible {@link net.kuujo.copycat.spi.protocol.Protocol protocol} support. The
 * Copycat replication implementation is completely protocol agnostic, so users can use Copycat
 * provided protocols or custom protocols. Each {@link Copycat} instance is thus associated with a
 * {@link net.kuujo.copycat.cluster.Cluster cluster} and {@link net.kuujo.copycat.spi.protocol.Protocol protocol}
 * which it uses for communication between replicas. It is very important that all nodes within the
 * same Copycat cluster use the same protocol for obvious reasons.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Copycat {
  protected final StateContext state;
  protected final Cluster cluster;
  protected final CopycatConfig config;
  protected final Events events;

  /**
   * Constructs a Copycat replica with a default configuration.
   *
   * @param stateMachine The Copycat state machine.
   * @param log The Copycat log.
   * @param cluster The Copycat cluster configuration.
   * @param protocol The asynchronous protocol.
   * @param <M> The cluster member type.
   * @throws NullPointerException if any arguments are null
   */
  public Copycat(StateMachine stateMachine, Log log, Cluster cluster, Protocol protocol) {
    this(stateMachine, log, cluster, protocol, new CopycatConfig());
  }

  /**
   * Constructs a Copycat replica with a user-defined configuration.
   *
   * @param stateMachine The Copycat state machine.
   * @param log The Copycat log.
   * @param cluster The Copycat cluster configuration.
   * @param protocol The asynchronous protocol.
   * @param config The replica configuration.
   * @param <M> The cluster member type.
   * @throws NullPointerException if any arguments are null
   */
  public Copycat(StateMachine stateMachine, Log log, Cluster cluster,
      Protocol protocol, CopycatConfig config) {
    this(new StateContext(stateMachine, log, cluster, protocol, config), cluster, config);
  }

  private Copycat(StateContext state, Cluster cluster, CopycatConfig config) {
    this.state = state;
    this.cluster = cluster;
    this.config = config;
    this.events = new DefaultEvents(state.events());
  }

  /**
   * Returns the replica configuration.
   *
   * @return The replica configuration.
   */
  public CopycatConfig config() {
    return config;
  }

  /**
   * Returns the cluster configuration.
   *
   * @return The cluster configuration.
   */
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Returns the context events.
   *
   * @return Context events.
   */
  public Events on() {
    return events;
  }

  /**
   * Returns the context for a specific event.
   *
   * @param event The event for which to return the context.
   * @return The event context.
   * @throws NullPointerException if {@code event} is null
   */
  public <T extends Event> EventContext<T> on(Class<T> event) {
    return events.event(Assert.isNotNull(event, "Event cannot be null"));
  }

  /**
   * Returns the event handlers registry.
   *
   * @return The event handlers registry.
   */
  public EventHandlers events() {
    return state.events();
  }

  /**
   * Returns an event handler registry for a specific event.
   *
   * @param event The event for which to return the registry.
   * @return An event handler registry.
   * @throws NullPointerException if {@code event} is null
   */
  public <T extends Event> EventHandlerRegistry<T> event(Class<T> event) {
    return state.events().event(Assert.isNotNull(event, "Event cannot be null"));
  }

  /**
   * Returns the current replica state.
   *
   * @return The current replica state.
   */
  public CopycatState state() {
    return state.state();
  }

  /**
   * Returns the current leader URI.
   *
   * @return The current leader URI.
   */
  public String leader() {
    return state.currentLeader();
  }

  /**
   * Returns a boolean indicating whether the node is the current leader.
   *
   * @return Indicates whether the node is the current leader.
   */
  public boolean isLeader() {
    return state.isLeader();
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

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
