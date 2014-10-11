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
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.spi.CorrelationStrategy;
import net.kuujo.copycat.spi.QuorumStrategy;
import net.kuujo.copycat.spi.TimerStrategy;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;

/**
 * Replica configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatConfig {
  private static final ThreadFactory THREAD_FACTORY = new NamedThreadFactory("config-timer-%s");
  private static final QuorumStrategy<?> DEFAULT_QUORUM_STRATEGY = (cluster) -> (int) Math.floor(cluster.members().size() / 2) + 1;
  private static final CorrelationStrategy<?> DEFAULT_CORRELATION_STRATEGY = () -> UUID.randomUUID().toString();

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);
  @SuppressWarnings("unchecked")
  private final TimerStrategy DEFAULT_TIMER_STRATEGY = (task, delay, unit) -> (ScheduledFuture<Void>) scheduler.schedule(task, delay, unit);
  private long electionTimeout = 2000;
  private long heartbeatInterval = 500;
  private boolean requireCommandQuorum = true;
  private int commandQuorumSize = -1;
  @SuppressWarnings("rawtypes")
  private QuorumStrategy commandQuorumStrategy = DEFAULT_QUORUM_STRATEGY;
  private boolean consistentCommandExecution = true;
  private boolean requireQueryQuorum = true;
  private int queryQuorumSize = -1;
  @SuppressWarnings("rawtypes")
  private QuorumStrategy queryQuorumStrategy = DEFAULT_QUORUM_STRATEGY;
  private boolean consistentQueryExecution = true;
  private int maxLogSize = 32 * 1024^2;
  private CorrelationStrategy<?> correlationStrategy = DEFAULT_CORRELATION_STRATEGY;
  private TimerStrategy timerStrategy = DEFAULT_TIMER_STRATEGY;

  /**
   * Sets the replica election timeout.
   * 
   * @param timeout The election timeout.
   * @throws IllegalArgumentException if {@code timeout} is not > 0
   */
  public void setElectionTimeout(long timeout) {
    this.electionTimeout = Assert.arg(timeout, timeout > 0, "Election timeout must be positive");
  }

  /**
   * Returns the replica election timeout.
   * 
   * @return The election timeout.
   */
  public long getElectionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the replica election timeout, returning the configuration for method chaining.
   * 
   * @param timeout The election timeout.
   * @return The copycat configuration.
   * @throws IllegalArgumentException if {@code timeout} is not > 0
   */
  public CopycatConfig withElectionTimeout(long timeout) {
    this.electionTimeout = Assert.arg(timeout, timeout > 0, "Election timeout must be positive");
    return this;
  }

  /**
   * Sets the replica heartbeat interval.
   * 
   * @param interval The interval at which the node should send heartbeat messages.
   * @throws IllegalArgumentException if {@code interval} is not > 0
   */
  public void setHeartbeatInterval(long interval) {
    this.heartbeatInterval = Assert.arg(interval, interval > 0, "Heart beat interval must be positive");
  }

  /**
   * Returns the replica heartbeat interval.
   * 
   * @return The replica heartbeat interval.
   */
  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Sets the replica heartbeat interval, returning the configuration for method chaining.
   * 
   * @param interval The interval at which the node should send heartbeat messages.
   * @return The replica configuration.
   * @throws IllegalArgumentException if {@code interval} is not > 0
   */
  public CopycatConfig withHeartbeatInterval(long interval) {
    this.heartbeatInterval = Assert.arg(interval, interval > 0, "Heart beat interval must be positive");
    return this;
  }

  /**
   * Sets whether a quorum replication is required for command operations.
   * 
   * @param require Indicates whether a quorum replication should be required for commands.
   */
  public void setRequireCommandQuorum(boolean require) {
    this.requireCommandQuorum = require;
  }

  /**
   * Returns a boolean indicating whether a quorum replication is required for command
   * operations.
   * 
   * @return Indicates whether a quorum replication is required for command operations.
   */
  public boolean isRequireCommandQuorum() {
    return requireCommandQuorum;
  }

  /**
   * Sets whether a quorum replication is required for command operations, returning the
   * configuration for method chaining.
   * 
   * @param require Indicates whether a quorum replication should be required for commands.
   * @return The Copycat configuration.
   */
  public CopycatConfig withRequireCommandQuorum(boolean require) {
    this.requireCommandQuorum = require;
    return this;
  }

  /**
   * Sets the required command quorum size.
   *
   * @param quorumSize The required command quorum size.
   * @throws IllegalArgumentException if {@code quorumSize} is not > -1
   */
  public void setCommandQuorumSize(int quorumSize) {
    this.commandQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.commandQuorumStrategy = (config) -> commandQuorumSize;
  }

  /**
   * Returns the required command quorum size.
   *
   * @return The required command quorum size. Defaults to <code>null</code>
   */
  public int getCommandQuorumSize() {
    return commandQuorumSize;
  }

  /**
   * Sets the required command quorum size, returning the configuration for method chaining.
   *
   * @param quorumSize The required command quorum size.
   * @return The copycat configuration.
   * @throws IllegalArgumentException if {@code quorumSize} is not > -1
   */
  public CopycatConfig withCommandQuorumSize(int quorumSize) {
    this.commandQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.commandQuorumStrategy = (config) -> commandQuorumSize;
    return this;
  }

  /**
   * Sets the cluster command quorum strategy.
   *
   * @param strategy The cluster command quorum calculation strategy.
   * @throws NullPointerException if {@code strategy} is null
   */
  public void setCommandQuorumStrategy(QuorumStrategy<?> strategy) {
    this.commandQuorumStrategy = Assert.isNotNull(strategy, "strategy");
  }

  /**
   * Returns the cluster command quorum strategy.
   *
   * @return The cluster command quorum calculation strategy.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <C extends Cluster> QuorumStrategy<C> getCommandQuorumStrategy() {
    return commandQuorumStrategy;
  }

  /**
   * Sets the cluster command quorum strategy, returning the configuration for method chaining.
   *
   * @param strategy The cluster command quorum calculation strategy.
   * @return The copycat configuration.
   * @throws NullPointerException if {@code strategy} is null
   */
  public CopycatConfig withCommandQuorumStrategy(QuorumStrategy<?> strategy) {
    this.commandQuorumStrategy = Assert.isNotNull(strategy, "strategy");
    return this;
  }

  /**
   * Sets whether to use consistent command execution.
   *
   * @param consistent Whether to use consistent command execution.
   */
  public void setConsistentCommandExecution(boolean consistent) {
    this.consistentCommandExecution = consistent;
    if (consistent) {
      setRequireCommandQuorum(true);
      this.commandQuorumStrategy = DEFAULT_QUORUM_STRATEGY;
    } else {
      setRequireCommandQuorum(false);
    }
  }

  /**
   * Returns whether consistent command execution is enabled.
   *
   * @return Indicates whether consistent command execution is enabled.
   */
  public boolean isConsistentCommandExecution() {
    return consistentCommandExecution;
  }

  /**
   * Sets whether to use consistent command execution, returning the configuration for method chaining.
   *
   * @param consistent Whether to use consistent command execution.
   * @return The Copycat configuration.
   */
  public CopycatConfig withConsistentCommandExecution(boolean consistent) {
    setConsistentCommandExecution(consistent);
    return this;
  }

  /**
   * Sets whether a quorum synchronization is required for query operations.
   *
   * @param require Indicates whether a quorum synchronization should be required for query
   *          operations.
   */
  public void setRequireQueryQuorum(boolean require) {
    this.requireQueryQuorum = require;
  }

  /**
   * Returns a boolean indicating whether a quorum synchronization is required for query
   * operations.
   *
   * @return Indicates whether a quorum synchronization is required for query operations.
   */
  public boolean isRequireQueryQuorum() {
    return requireQueryQuorum;
  }

  /**
   * Sets whether a quorum synchronization is required for query operations, returning
   * the configuration for method chaining.
   *
   * @param require Indicates whether a quorum synchronization should be required for query
   *          operations.
   * @return The replica configuration.
   */
  public CopycatConfig withRequireQueryQuorum(boolean require) {
    this.requireQueryQuorum = require;
    return this;
  }

  /**
   * Sets the required query quorum size.
   *
   * @param quorumSize The required query quorum size.
   * @throws IllegalArgumentException if {@code quorumSize} is not > -1
   */
  public void setQueryQuorumSize(int quorumSize) {
    this.queryQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.queryQuorumStrategy = (config) -> queryQuorumSize;
  }

  /**
   * Returns the required query quorum size.
   *
   * @return The required query quorum size. Defaults to <code>null</code>
   */
  public int getQueryQuorumSize() {
    return queryQuorumSize;
  }

  /**
   * Sets the required query quorum size, returning the configuration for method chaining.
   *
   * @param quorumSize The required query quorum size.
   * @return The copycat configuration.
   * @throws IllegalArgumentException if {@code quorumSize} is not > -1
   */
  public CopycatConfig withQueryQuorumSize(int quorumSize) {
    this.queryQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.queryQuorumStrategy = (config) -> queryQuorumSize;
    return this;
  }

  /**
   * Sets the cluster query quorum strategy.
   *
   * @param strategy The cluster query quorum calculation strategy.
   * @throws NullPointerException if {@code strategy} is null
   */
  public void setQueryQuorumStrategy(QuorumStrategy<?> strategy) {
    this.queryQuorumStrategy = Assert.isNotNull(strategy, "strategy");
  }

  /**
   * Returns the cluster query quorum strategy.
   *
   * @return The cluster query quorum calculation strategy.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <C extends Cluster> QuorumStrategy<C> getQueryQuorumStrategy() {
    return queryQuorumStrategy;
  }

  /**
   * Sets the cluster query quorum strategy, returning the configuration for method chaining.
   *
   * @param strategy The cluster query quorum calculation strategy.
   * @return The Copycat configuration.
   * @throws NullPointerException if {@code strategy} is null
   */
  public CopycatConfig withQueryQuorumStrategn(QuorumStrategy<?> strategy) {
    this.queryQuorumStrategy = Assert.isNotNull(strategy, "strategy");
    return this;
  }

  /**
   * Sets whether to use consistent query execution.
   *
   * @param consistent Whether to use consistent query execution.
   */
  public void setConsistentQueryExecution(boolean consistent) {
    this.consistentQueryExecution = consistent;
    if (consistent) {
      setRequireQueryQuorum(true);
      this.queryQuorumStrategy = DEFAULT_QUORUM_STRATEGY;
    } else {
      setRequireQueryQuorum(false);
    }
  }

  /**
   * Returns whether consistent query execution is enabled.
   *
   * @return Indicates whether consistent query execution is enabled.
   */
  public boolean isConsistentQueryExecution() {
    return consistentQueryExecution;
  }

  /**
   * Sets whether to use consistent query execution, returning the configuration for method chaining.
   *
   * @param consistent Whether to use consistent query execution.
   * @return The Copycat configuration.
   */
  public CopycatConfig withConsistentQueryExecution(boolean consistent) {
    setConsistentQueryExecution(consistent);
    return this;
  }

  /**
   * Sets the maximum log size.
   *
   * @param maxSize The maximum local log size.
   * @throws IllegalArgumentException if {@code maxSize} is not > 0
   */
  public void setMaxLogSize(int maxSize) {
    this.maxLogSize = Assert.arg(maxSize, maxSize > 0, "Max log size must be positive");
  }

  /**
   * Returns the maximum log size.
   *
   * @return The maximum local log size.
   */
  public int getMaxLogSize() {
    return maxLogSize;
  }

  /**
   * Sets the maximum log size, returning the configuration for method chaining.
   *
   * @param maxSize The maximum local log size.
   * @return The replica configuration.
   * @throws IllegalArgumentException if {@code maxSize} is not > 0
   */
  public CopycatConfig withMaxLogSize(int maxSize) {
    this.maxLogSize = Assert.arg(maxSize, maxSize > 0, "Max log size must be positive");
    return this;
  }

  /**
   * Sets the message correlation strategy.
   *
   * @param strategy The message correlation strategy.
   * @throws NullPointerException if {@code strategy} is null
   */
  public void setCorrelationStrategy(CorrelationStrategy<?> strategy) {
    this.correlationStrategy = Assert.isNotNull(strategy, "strategy");
  }

  /**
   * Returns the message correlation strategy.
   *
   * @return The message correlation strategy.
   */
  public CorrelationStrategy<?> getCorrelationStrategy() {
    return correlationStrategy;
  }

  /**
   * Sets the message correlation strategy, returning the configuration for method chaining.
   *
   * @param strategy The message correlation strategy.
   * @return The copycat configuration.
   * @throws NullPointerException if {@code strategy} is null
   */
  public CopycatConfig withCorrelationStrategy(CorrelationStrategy<?> strategy) {
    this.correlationStrategy = Assert.isNotNull(strategy, "strategy");
    return this;
  }

  /**
   * Sets the timer strategy.
   *
   * @param strategy The timer strategy.
   * @throws NullPointerException if {@code strategy} is null
   */
  public void setTimerStrategy(TimerStrategy strategy) {
    this.timerStrategy = Assert.isNotNull(strategy, "strategy");
  }

  /**
   * Returns the timer strategy.
   *
   * @return The timer strategy.
   */
  public TimerStrategy getTimerStrategy() {
    return timerStrategy;
  }

  /**
   * Sets the timer strategy, returning the configuration for method chaining.
   *
   * @param strategy The timer strategy.
   * @return The copycat configuration.
   * @throws NullPointerException if {@code strategy} is null
   */
  public CopycatConfig withTimerStrategy(TimerStrategy strategy) {
    this.timerStrategy = Assert.isNotNull(strategy, "strategy");
    return this;
  }

  @Override
  public String toString() {
    String value = "CopycatConfig";
    value += "[\n";
    value += String.format("electionTimeout=%d", electionTimeout);
    value += ",\n";
    value += String.format("heartbeatInterval=%d", heartbeatInterval);
    value += ",\n";
    value += String.format("requireQueryQuorum=%s", requireQueryQuorum);
    value += ",\n";
    value += String.format("queryQuorumSize=%d", queryQuorumSize);
    value += ",\n";
    value += String.format("queryQuorumStrategy=%s", queryQuorumStrategy);
    value += ",\n";
    value += String.format("consistentQueryExecution=%s", consistentQueryExecution);
    value += ",\n";
    value += String.format("requireCommandQuorum=%s", requireCommandQuorum);
    value += ",\n";
    value += String.format("commandQuorumSize=%s", commandQuorumSize);
    value += ",\n";
    value += String.format("commandQuorumStrategy=%s", commandQuorumStrategy);
    value += ",\n";
    value += String.format("consistentCommandExecution=%s", consistentCommandExecution);
    value += ",\n";
    value += String.format("maxLogSize=%d", maxLogSize);
    value += ",\n";
    value += String.format("correlationStrategy=%s", correlationStrategy);
    value += ",\n";
    value += String.format("timerStrategy=%s", timerStrategy);
    value += "\n]";
    return value;
  }

}
