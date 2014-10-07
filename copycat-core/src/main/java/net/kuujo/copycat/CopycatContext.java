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
import net.kuujo.copycat.event.*;
import net.kuujo.copycat.internal.DefaultCopycatContext;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.spi.CopycatContextFactory;
import net.kuujo.copycat.spi.CorrelationStrategy;
import net.kuujo.copycat.spi.QuorumStrategy;
import net.kuujo.copycat.spi.TimerStrategy;
import net.kuujo.copycat.spi.protocol.CopycatProtocol;

import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;

/**
 * Copycat context.<p>
 *
 * The Copycat context is the core of Copycat's functionality. Contexts are startable objects that, once started,
 * accept commands via the {@link CopycatContext#submitCommand(String, Object...)} method. Each context contains a
 * {@link net.kuujo.copycat.StateMachine}, {@link net.kuujo.copycat.log.Log}, and
 * {@link net.kuujo.copycat.cluster.Cluster}, each of which are required for the operation of the system.<p>
 *
 * {@code
 * StateMachine stateMachine = new MyStateMachine();
 * Log log = new MemoryMappedFileLog("data.log");
 * ClusterConfig<Member> config = new LocalClusterConfig();
 * config.setLocalMember("foo");
 * config.setRemoteMembers("bar", "baz");
 * Cluster<Member> cluster = new LocalCluster(config);
 * CopycatContext context = CopycatContext.context(stateMachine, log, cluster);
 * context.start();
 * context.submitCommand("put", "foo").thenRun(() -> System.out.println("PUT 'foo'"));
 * }
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CopycatContext {
  static final CopycatContextFactory factory = ServiceLoader.load(CopycatContextFactory.class).iterator().next();

  /**
   * Returns a new context builder.
   *
   * @return A new copycat context builder.
   */
  static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a new Copycat context.
   *
   * @param stateMachine The Copycat state machine.
   * @param cluster The Copycat cluster.
   * @return A new Copycat context.
   */
  static CopycatContext context(StateMachine stateMachine, Cluster<?> cluster) {
    return factory.createContext(stateMachine, new InMemoryLog(), cluster, new CopycatConfig());
  }

  /**
   * Creates a new Copycat context.
   *
   * @param stateMachine The Copycat state machine.
   * @param log The Copycat log.
   * @param cluster The Copycat cluster.
   * @return A new Copycat context.
   */
  static CopycatContext context(StateMachine stateMachine, Log log, Cluster<?> cluster) {
    return factory.createContext(stateMachine, log, cluster, new CopycatConfig());
  }

  /**
   * Creates a new Copycat context.
   *
   * @param stateMachine The Copycat state machine.
   * @param log The Copycat log.
   * @param cluster The Copycat cluster.
   * @param config The Copycat configuration.
   * @return A new Copycat context.
   */
  static CopycatContext context(StateMachine stateMachine, Log log, Cluster<?> cluster, CopycatConfig config) {
    return factory.createContext(stateMachine, log, cluster, config);
  }

  /**
   * Returns the replica configuration.
   *
   * @return The replica configuration.
   */
  CopycatConfig config();

  /**
   * Returns the cluster configuration.
   *
   * @return The cluster configuration.
   */
  <M extends Member> Cluster<M> cluster();

  /**
   * Returns the context events.
   *
   * @return Context events.
   */
  Events on();

  /**
   * Returns the context for a specific event.
   *
   * @param event The event for which to return the context.
   * @return The event context.
   */
  <T extends Event> EventContext<T> on(Class<T> event);

  /**
   * Returns the event handlers registry.
   *
   * @return The event handlers registry.
   */
  EventHandlers events();

  /**
   * Returns an event handler registry for a specific event.
   *
   * @param event The event for which to return the registry.
   * @return The event handler registry.
   */
  <T extends Event> EventHandlerRegistry<T> event(Class<T> event);

  /**
   * Returns the current replica state.
   *
   * @return The current replica state.
   */
  CopycatState state();

  /**
   * Returns the current leader URI.
   *
   * @return The current leader URI.
   */
  String leader();

  /**
   * Returns a boolean indicating whether the node is the current leader.
   *
   * @return Indicates whether the node is the current leader.
   */
  boolean isLeader();

  /**
   * Starts the context.
   *
   * @return A completable future to be completed once the context has started.
   */
  CompletableFuture<Void> start();

  /**
   * Stops the context.
   *
   * @return A completable future that will be completed when the context has started.
   */
  CompletableFuture<Void> stop();

  /**
   * Submits a command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param args An ordered list of command arguments.
   * @return A completable future to be completed once the result is received.
   */
  <R> CompletableFuture<R> submitCommand(final String command, final Object... args);

  /**
   * Copycat context builder.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  @SuppressWarnings("rawtypes")
  public static class Builder {
    private CopycatConfig config = new CopycatConfig();
    private Cluster cluster;
    private CopycatProtocol protocol;
    private StateMachine stateMachine;
    private Log log = new InMemoryLog();

    private Builder() {
    }

    /**
     * Sets the copycat log.
     *
     * @param log The copycat log.
     * @return The copycat builder.
     */
    public Builder withLog(Log log) {
      this.log = log;
      return this;
    }

    /**
     * Sets the copycat configuration.
     *
     * @param config The copycat configuration.
     * @return The copycat builder.
     */
    public Builder withConfig(CopycatConfig config) {
      this.config = config;
      return this;
    }

    /**
     * Sets the copycat election timeout.
     *
     * @param timeout The copycat election timeout.
     * @return The copycat builder.
     */
    public Builder withElectionTimeout(long timeout) {
      config.setElectionTimeout(timeout);
      return this;
    }

    /**
     * Sets the copycat heartbeat interval.
     *
     * @param interval The copycat heartbeat interval.
     * @return The copycat builder.
     */
    public Builder withHeartbeatInterval(long interval) {
      config.setHeartbeatInterval(interval);
      return this;
    }

    /**
     * Sets whether to require quorums during reads.
     *
     * @param requireQuorum Whether to require quorums during reads.
     * @return The copycat builder.
     */
    public Builder withRequireReadQuorum(boolean requireQuorum) {
      config.setRequireReadQuorum(requireQuorum);
      return this;
    }

    /**
     * Sets the read quorum size.
     *
     * @param quorumSize The read quorum size.
     * @return The copycat builder.
     */
    public Builder withReadQuorumSize(int quorumSize) {
      config.setReadQuorumSize(quorumSize);
      return this;
    }

    /**
     * Sets the read quorum strategy.
     *
     * @param quorumStrategy The read quorum strategy.
     * @return The copycat builder.
     */
    public Builder withReadQuorumStrategy(QuorumStrategy quorumStrategy) {
      config.setReadQuorumStrategy(quorumStrategy);
      return this;
    }

    /**
     * Sets whether to require quorums during writes.
     *
     * @param requireQuorum Whether to require quorums during writes.
     * @return The copycat builder.
     */
    public Builder withRequireWriteQuorum(boolean requireQuorum) {
      config.setRequireWriteQuorum(requireQuorum);
      return this;
    }

    /**
     * Sets the write quorum size.
     *
     * @param quorumSize The write quorum size.
     * @return The copycat builder.
     */
    public Builder withWriteQuorumSize(int quorumSize) {
      config.setWriteQuorumSize(quorumSize);
      return this;
    }

    /**
     * Sets the write quorum strategy.
     *
     * @param quorumStrategy The write quorum strategy.
     * @return The copycat builder.
     */
    public Builder withWriteQuorumStrategy(QuorumStrategy quorumStrategy) {
      config.setWriteQuorumStrategy(quorumStrategy);
      return this;
    }

    /**
     * Sets the max log size.
     *
     * @param maxSize The max log size.
     * @return The copycat builder.
     */
    public Builder withMaxLogSize(int maxSize) {
      config.setMaxLogSize(maxSize);
      return this;
    }

    /**
     * Sets the correlation strategy.
     *
     * @param strategy The correlation strategy.
     * @return The copycat builder.
     */
    public Builder withCorrelationStrategy(CorrelationStrategy<?> strategy) {
      config.setCorrelationStrategy(strategy);
      return this;
    }

    /**
     * Sets the timer strategy.
     *
     * @param strategy The timer strategy.
     * @return The copycat builder.
     */
    public Builder withTimerStrategy(TimerStrategy strategy) {
      config.setTimerStrategy(strategy);
      return this;
    }

    /**
     * Sets the cluster protocol.
     *
     * @param protocol The cluster protocol.
     * @return The copycat builder.
     */
    public Builder withProtocol(CopycatProtocol<?> protocol) {
      this.protocol = protocol;
      return this;
    }

    /**
     * Sets the copycat cluster.
     *
     * @param cluster The copycat cluster.
     * @return The copycat builder.
     */
    public Builder withCluster(Cluster<?> cluster) {
      this.cluster = cluster;
      return this;
    }

    /**
     * Sets the copycat state machine.
     *
     * @param stateMachine The state machine.
     * @return The copycat builder.
     */
    public Builder withStateMachine(StateMachine stateMachine) {
      this.stateMachine = stateMachine;
      return this;
    }

    /**
     * Builds the copycat instance.
     *
     * @return The copycat instance.
     */
    public CopycatContext build() {
      return context(stateMachine, log, cluster, config);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }

  }

}
