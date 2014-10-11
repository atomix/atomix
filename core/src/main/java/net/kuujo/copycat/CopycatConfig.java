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
  
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);
  private long electionTimeout = 2000;
  private long heartbeatInterval = 500;
  private boolean requireCommandQuorum = true;
  private int commandQuorumSize = -1;
  @SuppressWarnings("rawtypes")
  private QuorumStrategy commandQuorumStrategy = (cluster) -> (int) Math.floor(cluster.members().size() / 2) + 1;
  private int maxLogSize = 32 * 1024^2;
  private boolean requireQueryQuorum = true;
  private int queryQuorumSize = -1;
  @SuppressWarnings("rawtypes")
  private QuorumStrategy queryQuorumStrategy = (cluster) -> (int) Math.floor(cluster.members().size() / 2) + 1;
  private CorrelationStrategy<?> correlationStrategy = () -> UUID.randomUUID().toString();
  @SuppressWarnings("unchecked")
  private TimerStrategy timerStrategy = (task, delay, unit) -> (ScheduledFuture<Void>) scheduler.schedule(task, delay, unit);

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
   * Sets whether a quorum replication is required for write operations.
   * 
   * @param require Indicates whether a quorum replication should be required for writes.   
   */
  public void setRequireCommandQuorum(boolean require) {
    this.requireCommandQuorum = require;
  }

  /**
   * Returns a boolean indicating whether a quorum replication is required for write
   * operations.
   * 
   * @return Indicates whether a quorum replication is required for write operations.
   */
  public boolean isRequireCommandQuorum() {
    return requireCommandQuorum;
  }

  /**
   * Sets whether a quorum replication is required for write operations, returning the
   * configuration for method chaining.
   * 
   * @param require Indicates whether a quorum replication should be required for writes.
   * @return The replica configuration.
   */
  public CopycatConfig withRequireWriteQuorum(boolean require) {
    this.requireCommandQuorum = require;
    return this;
  }

  /**
   * Sets the required write quorum size.
   *
   * @param quorumSize The required write quorum size.
   * @throws IllegalArgumentException if {@code quorumSize} is not > -1
   */
  public void setCommandQuorumSize(int quorumSize) {
    this.commandQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.commandQuorumStrategy = (config) -> commandQuorumSize;
  }

  /**
   * Returns the required write quorum size.
   *
   * @return The required write quorum size. Defaults to <code>null</code>
   */
  public int getCommandQuorumSize() {
    return commandQuorumSize;
  }

  /**
   * Sets the required write quorum size, returning the configuration for method chaining.
   *
   * @param quorumSize The required write quorum size.
   * @return The copycat configuration.
   * @throws IllegalArgumentException if {@code quorumSize} is not > -1
   */
  public CopycatConfig withWriteQuorumSize(int quorumSize) {
    this.commandQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.commandQuorumStrategy = (config) -> commandQuorumSize;
    return this;
  }

  /**
   * Sets whether a quorum synchronization is required for read operations.
   * 
   * @param require Indicates whether a quorum synchronization should be required for read
   *          operations.
   */
  public void setRequireQueryQuorum(boolean require) {
    this.requireQueryQuorum = require;
  }

  /**
   * Returns a boolean indicating whether a quorum synchronization is required for read
   * operations.
   * 
   * @return Indicates whether a quorum synchronization is required for read operations.
   */
  public boolean isRequireQueryQuorum() {
    return requireQueryQuorum;
  }

  /**
   * Sets whether a quorum synchronization is required for read operations, returning
   * the configuration for method chaining.
   * 
   * @param require Indicates whether a quorum synchronization should be required for read
   *          operations.
   * @return The replica configuration.
   */
  public CopycatConfig withRequireReadQuorum(boolean require) {
    this.requireQueryQuorum = require;
    return this;
  }

  /**
   * Sets the cluster write quorum strategy.
   *
   * @param strategy The cluster write quorum calculation strategy.
   * @throws NullPointerException if {@code strategy} is null
   */
  public void setCommandQuorumStrategy(QuorumStrategy<?> strategy) {
    this.commandQuorumStrategy = Assert.isNotNull(strategy, "strategy");
  }

  /**
   * Returns the cluster write quorum strategy.
   *
   * @return The cluster write quorum calculation strategy.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <C extends Cluster> QuorumStrategy<C> getCommandQuorumStrategy() {
    return commandQuorumStrategy;
  }

  /**
   * Sets the cluster write quorum strategy, returning the configuration for method chaining.
   *
   * @param strategy The cluster write quorum calculation strategy.
   * @return The copycat configuration.
   * @throws NullPointerException if {@code strategy} is null
   */
  public CopycatConfig withWriteQuorumStrategy(QuorumStrategy<?> strategy) {
    this.commandQuorumStrategy = Assert.isNotNull(strategy, "strategy");
    return this;
  }

  /**
   * Sets the required read quorum size.
   *
   * @param quorumSize The required read quorum size.
   * @throws IllegalArgumentException if {@code quorumSize} is not > -1
   */
  public void setQueryQuorumSize(int quorumSize) {
    this.queryQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.queryQuorumStrategy = (config) -> queryQuorumSize;
  }

  /**
   * Returns the required read quorum size.
   *
   * @return The required read quorum size. Defaults to <code>null</code>
   */
  public int getQueryQuorumSize() {
    return queryQuorumSize;
  }

  /**
   * Sets the required read quorum size, returning the configuration for method chaining.
   *
   * @param quorumSize The required read quorum size.
   * @return The copycat configuration.
   * @throws IllegalArgumentException if {@code quorumSize} is not > -1
   */
  public CopycatConfig withReadQuorumSize(int quorumSize) {
    this.queryQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.queryQuorumStrategy = (config) -> queryQuorumSize;
    return this;
  }

  /**
   * Sets the cluster read quorum strategy.
   *
   * @param strategy The cluster read quorum calculation strategy.
   * @throws NullPointerException if {@code strategy} is null
   */
  public void setQueryQuorumStrategy(QuorumStrategy<?> strategy) {
    this.queryQuorumStrategy = Assert.isNotNull(strategy, "strategy");
  }

  /**
   * Returns the cluster read quorum strategy.
   *
   * @return The cluster read quorum calculation strategy.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <C extends Cluster> QuorumStrategy<C> getQueryQuorumStrategy() {
    return queryQuorumStrategy;
  }

  /**
   * Sets the cluster read quorum strategy, returning the configuration for method chaining.
   *
   * @param strategy The cluster read quorum calculation strategy.
   * @return The copycat configuration.
   * @throws NullPointerException if {@code strategy} is null
   */
  public CopycatConfig withReadQuorumStrategy(QuorumStrategy<?> strategy) {
    this.queryQuorumStrategy = Assert.isNotNull(strategy, "strategy");
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
    value += String.format("requireCommandQuorum=%s", requireCommandQuorum);
    value += ",\n";
    value += String.format("commandQuorumSize=%s", commandQuorumSize);
    value += ",\n";
    value += String.format("commandQuorumStrategy=%s", commandQuorumStrategy);
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
