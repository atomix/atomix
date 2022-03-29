// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.utils.config.Config;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster membership configuration.
 */
public class MembershipConfig implements Config {
  private static final int DEFAULT_BROADCAST_INTERVAL = 100;
  private static final int DEFAULT_REACHABILITY_TIMEOUT = 10000;
  private static final int DEFAULT_REACHABILITY_THRESHOLD = 10;

  private Duration broadcastInterval = Duration.ofMillis(DEFAULT_BROADCAST_INTERVAL);
  private int reachabilityThreshold = DEFAULT_REACHABILITY_THRESHOLD;
  private Duration reachabilityTimeout = Duration.ofMillis(DEFAULT_REACHABILITY_TIMEOUT);

  /**
   * Returns the reachability broadcast interval.
   *
   * @return the reachability broadcast interval
   */
  public Duration getBroadcastInterval() {
    return broadcastInterval;
  }

  /**
   * Sets the reachability broadcast interval.
   *
   * @param broadcastInterval the reachability broadcast interval
   * @return the membership configuration
   */
  public MembershipConfig setBroadcastInterval(Duration broadcastInterval) {
    this.broadcastInterval = checkNotNull(broadcastInterval);
    return this;
  }

  /**
   * Returns the reachability failure detection threshold.
   *
   * @return the reachability failure detection threshold
   */
  public int getReachabilityThreshold() {
    return reachabilityThreshold;
  }

  /**
   * Sets the reachability failure detection threshold.
   *
   * @param reachabilityThreshold the reachability failure detection threshold
   * @return the membership configuration
   */
  public MembershipConfig setReachabilityThreshold(int reachabilityThreshold) {
    this.reachabilityThreshold = reachabilityThreshold;
    return this;
  }

  /**
   * Returns the reachability failure timeout.
   *
   * @return the reachability failure timeout
   */
  public Duration getReachabilityTimeout() {
    return reachabilityTimeout;
  }

  /**
   * Sets the reachability failure timeout.
   *
   * @param reachabilityTimeout the reachability failure timeout
   * @return the membership configuration
   */
  public MembershipConfig setReachabilityTimeout(Duration reachabilityTimeout) {
    this.reachabilityTimeout = checkNotNull(reachabilityTimeout);
    return this;
  }
}
