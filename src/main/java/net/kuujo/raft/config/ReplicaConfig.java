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
package net.kuujo.raft.config;

/**
 * A state configuration.
 *
 * @author Jordan Halterman
 */
public class ReplicaConfig {
  private long electionTimeout = 10000;
  private long heartbeatInterval = 5000;
  private boolean useAdaptiveTimeouts = true;
  private double adaptiveTimeoutThreshold = 4;
  private boolean requireWriteMajority = true;
  private boolean requireReadMajority = true;

  /**
   * Returns the replica election timeout.
   *
   * @return
   *   The replica election timeout.
   */
  public long getElectionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the leader election timeout.
   *
   * @param timeout
   *   The leader election timeout.
   * @return
   *   The replica configuration.
   */
  public ReplicaConfig setElectionTimeout(long timeout) {
    electionTimeout = timeout;
    return this;
  }

  /**
   * Returns the replica heartbeat interval.
   *
   * @return
   *   The replica heartbeat interval.
   */
  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Sets the replica heartbeat interval.
   *
   * @param interval
   *   The replica heartbeat interval.
   * @return
   *   The replica configuration.
   */
  public ReplicaConfig setHeartbeatInterval(long interval) {
    heartbeatInterval = interval;
    return this;
  }

  /**
   * Returns a boolean indicating whether adaptive timeouts are enabled.
   *
   * @return
   *   Indicates whether adaptive timeouts are enabled.
   */
  public boolean isUseAdaptiveTimeouts() {
    return useAdaptiveTimeouts;
  }

  /**
   * Indicates whether the replica should use adaptive timeouts.
   *
   * @param useAdaptive
   *   Indicates whether to use adaptive timeouts.
   * @return
   *   The replica configuration.
   */
  public ReplicaConfig useAdaptiveTimeouts(boolean useAdaptive) {
    useAdaptiveTimeouts = useAdaptive;
    return this;
  }

  /**
   * Returns the adaptive timeout threshold.
   *
   * @return
   *   The adaptive timeout threshold.
   */
  public double getAdaptiveTimeoutThreshold() {
    return adaptiveTimeoutThreshold;
  }

  /**
   * Sets the adaptive timeout threshold.
   *
   * @param threshold
   *   The adaptive timeout threshold.
   * @return
   *   The replica configuration.
   */
  public ReplicaConfig setAdaptiveTimeoutThreshold(double threshold) {
    adaptiveTimeoutThreshold = threshold;
    return this;
  }

  /**
   * Returns a boolean indicating whether majority replication is required
   * for write operations.
   *
   * @return
   *   Indicates whether majority replication is required for write operations.
   */
  public boolean isRequireWriteMajority() {
    return requireWriteMajority;
  }

  /**
   * Sets whether majority replication is required for write operations.
   *
   * @param require
   *   Indicates whether majority replication should be required for writes.
   * @return
   *   The replication configuration.
   */
  public ReplicaConfig setRequireWriteMajority(boolean require) {
    requireWriteMajority = require;
    return this;
  }

  /**
   * Returns a boolean indicating whether majority synchronization is required
   * for read operations.
   *
   * @return
   *   Indicates whether majority synchronization is required for read operations.
   */
  public boolean isRequireReadMajority() {
    return requireReadMajority;
  }

  /**
   * Sets whether majority synchronization is required for read operations.
   *
   * @param require
   *   Indicates whether majority synchronization should be required for read operations.
   * @return
   *   The replication configuration.
   */
  public ReplicaConfig setRequireReadMajority(boolean require) {
    requireReadMajority = require;
    return this;
  }

}
