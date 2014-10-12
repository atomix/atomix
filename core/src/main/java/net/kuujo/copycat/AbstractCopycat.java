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

import java.util.function.Function;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.event.Event;
import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.EventHandlerRegistry;
import net.kuujo.copycat.event.EventHandlers;
import net.kuujo.copycat.event.Events;
import net.kuujo.copycat.internal.event.DefaultEvents;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.spi.CorrelationStrategy;
import net.kuujo.copycat.spi.QuorumStrategy;
import net.kuujo.copycat.spi.TimerStrategy;
import net.kuujo.copycat.spi.protocol.BaseProtocol;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractCopycat {
  protected final StateContext state;
  protected final Cluster<?> cluster;
  protected final CopycatConfig config;
  protected final Events events;

  /**
   * Copycat context builder.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  @SuppressWarnings("rawtypes")
  public static class Builder<T extends AbstractCopycat, P extends BaseProtocol<?>> {
    final Function<Builder, T> copycatFactory;
    CopycatConfig config = new CopycatConfig();
    Cluster cluster;
    P protocol;
    StateMachine stateMachine;
    Log log = new InMemoryLog();

    Builder(Function<Builder, T> copycatFactory) {
      this.copycatFactory = copycatFactory;
    }

    /**
     * Builds the copycat instance.
     *
     * @return The copycat instance.
     */
    public T build() {
      return copycatFactory.apply(this);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }

    /**
     * Sets the copycat cluster.
     *
     * @param cluster The copycat cluster.
     * @return The copycat builder.
     * @throws NullPointerException if {@code cluster} is null
     */
    public Builder<T, P> withCluster(Cluster<?> cluster) {
      this.cluster = Assert.isNotNull(cluster, "cluster");
      return this;
    }

    /**
     * Sets the copycat configuration.
     *
     * @param config The copycat configuration.
     * @return The copycat builder.
     * @throws NullPointerException if {@code config} is null
     */
    public Builder<T, P> withConfig(CopycatConfig config) {
      this.config = Assert.isNotNull(config, "config");
      return this;
    }

    /**
     * Sets the correlation strategy.
     *
     * @param strategy The correlation strategy.
     * @return The copycat builder.
     * @throws NullPointerException if {@code strategy} is null
     */
    public Builder<T, P> withCorrelationStrategy(CorrelationStrategy<?> strategy) {
      config.setCorrelationStrategy(strategy);
      return this;
    }

    /**
     * Sets the copycat election timeout.
     *
     * @param timeout The copycat election timeout.
     * @return The copycat builder.
     * @throws IllegalArgumentException if {@code timeout} is not > 0
     */
    public Builder<T, P> withElectionTimeout(long timeout) {
      config.setElectionTimeout(timeout);
      return this;
    }

    /**
     * Sets the copycat heartbeat interval.
     *
     * @param interval The copycat heartbeat interval.
     * @return The copycat builder.
     * @throws IllegalArgumentException if {@code interval} is not > 0
     */
    public Builder<T, P> withHeartbeatInterval(long interval) {
      config.setHeartbeatInterval(interval);
      return this;
    }

    /**
     * Sets the copycat log.
     *
     * @param log The copycat log.
     * @return The copycat builder.
     * @throws NullPointerException if {@code log} is null
     */
    public Builder<T, P> withLog(Log log) {
      this.log = Assert.isNotNull(log, "log");
      return this;
    }

    /**
     * Sets the max log size.
     *
     * @param maxSize The max log size.
     * @return The copycat builder.
     * @throws IllegalArgumentException if {@code maxSize} is not > 0
     */
    public Builder<T, P> withMaxLogSize(int maxSize) {
      config.setMaxLogSize(maxSize);
      return this;
    }

    /**
     * Sets the cluster protocol.
     *
     * @param protocol The cluster protocol.
     * @return The copycat builder.
     * @throws NullPointerException if {@code protocol} is null
     */
    public Builder<T, P> withProtocol(P protocol) {
      this.protocol = Assert.isNotNull(protocol, "protocol");
      return this;
    }

    /**
     * Sets the read quorum size.
     *
     * @param quorumSize The read quorum size.
     * @return The copycat builder.
     * @throws IllegalArgumentException if {@code quorumSize} is not > -1
     */
    public Builder<T, P> withReadQuorumSize(int quorumSize) {
      config.setQueryQuorumSize(quorumSize);
      return this;
    }

    /**
     * Sets the read quorum strategy.
     *
     * @param quorumStrategy The read quorum strategy.
     * @return The copycat builder.
     * @throws NullPointerException if {@code quorumStrategy} is null
     */
    public Builder<T, P> withReadQuorumStrategy(QuorumStrategy quorumStrategy) {
      config.setQueryQuorumStrategy(quorumStrategy);
      return this;
    }

    /**
     * Sets whether to require quorums during reads.
     *
     * @param requireQuorum Whether to require quorums during reads.
     * @return The copycat builder.
     */
    public Builder<T, P> withRequireReadQuorum(boolean requireQuorum) {
      config.setRequireQueryQuorum(requireQuorum);
      return this;
    }

    /**
     * Sets whether to require quorums during writes.
     *
     * @param requireQuorum Whether to require quorums during writes.
     * @return The copycat builder.
     */
    public Builder<T, P> withRequireWriteQuorum(boolean requireQuorum) {
      config.setRequireCommandQuorum(requireQuorum);
      return this;
    }

    /**
     * Sets the copycat state machine.
     *
     * @param stateMachine The state machine.
     * @return The copycat builder.
     * @throws NullPointerException if {@code stateMachine} is null
     */
    public Builder<T, P> withStateMachine(StateMachine stateMachine) {
      this.stateMachine = Assert.isNotNull(stateMachine, "stateMachine");
      return this;
    }

    /**
     * Sets the timer strategy.
     *
     * @param strategy The timer strategy.
     * @return The copycat builder.
     * @throws NullPointerException if {@code strategy} is null
     */
    public Builder<T, P> withTimerStrategy(TimerStrategy strategy) {
      config.setTimerStrategy(strategy);
      return this;
    }

    /**
     * Sets the write quorum size.
     *
     * @param quorumSize The write quorum size.
     * @return The copycat builder.
     * @throws IllegalArgumentException if {@code quorumSize} is not > -1
     */
    public Builder<T, P> withWriteQuorumSize(int quorumSize) {
      config.setCommandQuorumSize(quorumSize);
      return this;
    }

    /**
     * Sets the write quorum strategy.
     *
     * @param quorumStrategy The write quorum strategy.
     * @return The copycat builder.
     * @throws NullPointerException if {@code quorumStrategy} is null
     */
    public Builder<T, P> withWriteQuorumStrategy(QuorumStrategy quorumStrategy) {
      config.setCommandQuorumStrategy(quorumStrategy);
      return this;
    }
  }

  protected AbstractCopycat(StateContext state, Cluster<?> cluster, CopycatConfig config) {
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
  @SuppressWarnings("unchecked")
  public <M extends Member> Cluster<M> cluster() {
    return (Cluster<M>) cluster;
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

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
