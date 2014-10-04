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

import net.kuujo.copycat.spi.*;

/**
 * Replica configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatConfig {
  private long electionTimeout = 2000;
  private long heartbeatInterval = 500;
  private boolean requireWriteQuorum = true;
  private boolean requireReadQuorum = true;
  private int readQuorumSize = -1;
  private QuorumStrategy readQuorumStrategy = new MajorityQuorumStrategy();
  private int writeQuorumSize = -1;
  private QuorumStrategy writeQuorumStrategy = new MajorityQuorumStrategy();
  private int maxLogSize = 32 * 1024^2;
  private CorrelationStrategy<?> correlationStrategy = new UuidCorrelationStrategy();
  private TimerStrategy timerStrategy = new ThreadTimerStrategy();

  /**
   * Sets the replica election timeout.
   * 
   * @param timeout The election timeout.
   */
  public void setElectionTimeout(long timeout) {
    if (timeout < 0) throw new IllegalArgumentException("Election timeout must be positive");
    this.electionTimeout = timeout;
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
   */
  public CopycatConfig withElectionTimeout(long timeout) {
    if (timeout < 0) throw new IllegalArgumentException("Election timeout must be positive");
    this.electionTimeout = timeout;
    return this;
  }

  /**
   * Sets the replica heartbeat interval.
   * 
   * @param interval The interval at which the node should send heartbeat messages.
   */
  public void setHeartbeatInterval(long interval) {
    if (interval < 0) throw new IllegalArgumentException("Heart beat interval must be positive");
    this.heartbeatInterval = interval;
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
   */
  public CopycatConfig withHeartbeatInterval(long interval) {
    if (interval < 0) throw new IllegalArgumentException("Heart beat interval must be positive");
    this.heartbeatInterval = interval;
    return this;
  }

  /**
   * Sets whether a quorum replication is required for write operations.
   * 
   * @param require Indicates whether a quorum replication should be required for writes.   */
  public void setRequireWriteQuorum(boolean require) {
    this.requireWriteQuorum = require;
  }

  /**
   * Returns a boolean indicating whether a quorum replication is required for write
   * operations.
   * 
   * @return Indicates whether a quorum replication is required for write operations.
   */
  public boolean isRequireWriteQuorum() {
    return requireWriteQuorum;
  }

  /**
   * Sets whether a quorum replication is required for write operations, returning the
   * configuration for method chaining.
   * 
   * @param require Indicates whether a quorum replication should be required for writes.
   * @return The replica configuration.
   */
  public CopycatConfig withRequireWriteQuorum(boolean require) {
    this.requireWriteQuorum = require;
    return this;
  }

  /**
   * Sets the required write quorum size.
   *
   * @param quorumSize The required write quorum size.
   */
  public void setWriteQuorumSize(Integer quorumSize) {
    if (quorumSize < -1) throw new IllegalArgumentException("Quorum size must be -1 or greater");
    this.writeQuorumSize = quorumSize;
    this.writeQuorumStrategy = (config) -> writeQuorumSize;
  }

  /**
   * Returns the required write quorum size.
   *
   * @return The required write quorum size. Defaults to <code>null</code>
   */
  public Integer getWriteQuorumSize() {
    return writeQuorumSize;
  }

  /**
   * Sets the required write quorum size, returning the configuration for method chaining.
   *
   * @param quorumSize The required write quorum size.
   * @return The copycat configuration.
   */
  public CopycatConfig withWriteQuorumSize(Integer quorumSize) {
    if (quorumSize < -1) throw new IllegalArgumentException("Quorum size must be -1 or greater");
    this.writeQuorumSize = quorumSize;
    this.writeQuorumStrategy = (config) -> writeQuorumSize;
    return this;
  }

  /**
   * Sets whether a quorum synchronization is required for read operations.
   * 
   * @param require Indicates whether a quorum synchronization should be required for read
   *          operations.
   */
  public void setRequireReadQuorum(boolean require) {
    this.requireReadQuorum = require;
  }

  /**
   * Returns a boolean indicating whether a quorum synchronization is required for read
   * operations.
   * 
   * @return Indicates whether a quorum synchronization is required for read operations.
   */
  public boolean isRequireReadQuorum() {
    return requireReadQuorum;
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
    this.requireReadQuorum = require;
    return this;
  }

  /**
   * Sets the cluster write quorum strategy.
   *
   * @param strategy The cluster write quorum calculation strategy.
   */
  public void setWriteQuorumStrategy(QuorumStrategy strategy) {
    if (strategy == null) throw new NullPointerException();
    this.writeQuorumStrategy = strategy;
  }

  /**
   * Returns the cluster write quorum strategy.
   *
   * @return The cluster write quorum calculation strategy.
   */
  public QuorumStrategy getWriteQuorumStrategy() {
    return writeQuorumStrategy;
  }

  /**
   * Sets the cluster write quorum strategy, returning the configuration for method chaining.
   *
   * @param strategy The cluster write quorum calculation strategy.
   * @return The copycat configuration.
   */
  public CopycatConfig withWriteQuorumStrategy(QuorumStrategy strategy) {
    if (strategy == null) throw new NullPointerException();
    this.writeQuorumStrategy = strategy;
    return this;
  }

  /**
   * Sets the required read quorum size.
   *
   * @param quorumSize The required read quorum size.
   */
  public void setReadQuorumSize(Integer quorumSize) {
    if (quorumSize < -1) throw new IllegalArgumentException("Quorum size must be -1 or greater");
    this.readQuorumSize = quorumSize;
    this.readQuorumStrategy = (config) -> readQuorumSize;
  }

  /**
   * Returns the required read quorum size.
   *
   * @return The required read quorum size. Defaults to <code>null</code>
   */
  public Integer getReadQuorumSize() {
    return readQuorumSize;
  }

  /**
   * Sets the required read quorum size, returning the configuration for method chaining.
   *
   * @param quorumSize The required read quorum size.
   * @return The copycat configuration.
   */
  public CopycatConfig withReadQuorumSize(Integer quorumSize) {
    if (quorumSize < -1) throw new IllegalArgumentException("Quorum size must be -1 or greater");
    this.readQuorumSize = quorumSize;
    this.readQuorumStrategy = (config) -> readQuorumSize;
    return this;
  }

  /**
   * Sets the cluster read quorum strategy.
   *
   * @param strategy The cluster read quorum calculation strategy.
   */
  public void setReadQuorumStrategy(QuorumStrategy strategy) {
    if (strategy == null) throw new NullPointerException();
    this.readQuorumStrategy = strategy;
  }

  /**
   * Returns the cluster read quorum strategy.
   *
   * @return The cluster read quorum calculation strategy.
   */
  public QuorumStrategy getReadQuorumStrategy() {
    return readQuorumStrategy;
  }

  /**
   * Sets the cluster read quorum strategy, returning the configuration for method chaining.
   *
   * @param strategy The cluster read quorum calculation strategy.
   * @return The copycat configuration.
   */
  public CopycatConfig withReadQuorumStrategy(QuorumStrategy strategy) {
    if (strategy == null) throw new NullPointerException();
    this.readQuorumStrategy = strategy;
    return this;
  }

  /**
   * Sets the maximum log size.
   *
   * @param maxSize The maximum local log size.
   */
  public void setMaxLogSize(int maxSize) {
    if (maxSize < 0) throw new IllegalArgumentException("Log max size must be positive");
    this.maxLogSize = maxSize;
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
   */
  public CopycatConfig withMaxLogSize(int maxSize) {
    if (maxSize < 0) throw new IllegalArgumentException("Log max size must be positive");
    this.maxLogSize = maxSize;
    return this;
  }

  /**
   * Sets the message correlation strategy.
   *
   * @param strategy The message correlation strategy.
   */
  public void setCorrelationStrategy(CorrelationStrategy<?> strategy) {
    if (strategy == null) throw new NullPointerException();
    this.correlationStrategy = strategy;
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
   */
  public CopycatConfig withCorrelationStrategy(CorrelationStrategy<?> strategy) {
    if (strategy == null) throw new NullPointerException();
    this.correlationStrategy = strategy;
    return this;
  }

  /**
   * Sets the timer strategy.
   *
   * @param strategy The timer strategy.
   */
  public void setTimerStrategy(TimerStrategy strategy) {
    if (strategy == null) throw new NullPointerException();
    this.timerStrategy = strategy;
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
   */
  public CopycatConfig withTimerStrategy(TimerStrategy strategy) {
    this.timerStrategy = strategy;
    return this;
  }

}
