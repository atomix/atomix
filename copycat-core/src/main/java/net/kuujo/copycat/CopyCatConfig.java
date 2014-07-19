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

/**
 * Replica configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopyCatConfig {
  private long electionTimeout = 2000;
  private long heartbeatInterval = 1000;
  private boolean useAdaptiveTimeouts = false;
  private double adaptiveTimeoutThreshold = 10;
  private boolean requireWriteQuorum = true;
  private boolean requireReadQuorum = true;
  private int maxLogSize = 32 * 1024^2;

  /**
   * Sets the replica election timeout.
   * 
   * @param timeout The election timeout.
   * @return The replica configuration.
   */
  public CopyCatConfig setElectionTimeout(long timeout) {
    this.electionTimeout = timeout;
    return this;
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
   * Sets the replica heartbeat interval.
   * 
   * @param interval The interval at which the node should send heartbeat messages.
   * @return The replica configuration.
   */
  public CopyCatConfig setHeartbeatInterval(long interval) {
    this.heartbeatInterval = interval;
    return this;
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
   * Returns a boolean indicating whether adaptive timeouts are enabled.
   * 
   * @return Indicates whether adaptive timeouts are enabled.
   */
  public boolean isUseAdaptiveTimeouts() {
    return useAdaptiveTimeouts;
  }

  /**
   * Indicates whether the replica should use adaptive timeouts.
   * 
   * @param useAdaptive Indicates whether to use adaptive timeouts.
   * @return The replica configuration.
   */
  public CopyCatConfig setUseAdaptiveTimeouts(boolean useAdaptive) {
    this.useAdaptiveTimeouts = useAdaptive;
    return this;
  }

  /**
   * Returns the adaptive timeout threshold.
   * 
   * @return The adaptive timeout threshold.
   */
  public double getAdaptiveTimeoutThreshold() {
    return adaptiveTimeoutThreshold;
  }

  /**
   * Sets the adaptive timeout threshold.
   * 
   * @param threshold The adaptive timeout threshold.
   * @return The replica configuration.
   */
  public CopyCatConfig setAdaptiveTimeoutThreshold(double threshold) {
    this.adaptiveTimeoutThreshold = threshold;
    return this;
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
   * Sets whether a quorum replication is required for write operations.
   * 
   * @param require Indicates whether a quorum replication should be required for writes.
   * @return The replica configuration.
   */
  public CopyCatConfig setRequireWriteQuorum(boolean require) {
    this.requireWriteQuorum = require;
    return this;
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
   * Sets whether a quorum synchronization is required for read operations.
   * 
   * @param require Indicates whether a quorum synchronization should be required for read
   *          operations.
   * @return The replica configuration.
   */
  public CopyCatConfig setRequireReadQuorum(boolean require) {
    this.requireReadQuorum = require;
    return this;
  }

  /**
   * Sets the maximum log size.
   *
   * @param maxSize The maximum local log size.
   * @return The replica configuration.
   */
  public CopyCatConfig setMaxLogSize(int maxSize) {
    this.maxLogSize = maxSize;
    return this;
  }

  /**
   * Returns the maximum log size.
   *
   * @return The maximum local log size.
   */
  public int getMaxLogSize() {
    return maxLogSize;
  }

}
