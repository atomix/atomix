// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.discovery;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Multicast discovery configuration.
 */
public class MulticastDiscoveryConfig extends NodeDiscoveryConfig {
  private static final int DEFAULT_BROADCAST_INTERVAL = 1000;
  private static final int DEFAULT_FAILURE_TIMEOUT = 10000;
  private static final int DEFAULT_PHI_FAILURE_THRESHOLD = 10;

  private Duration broadcastInterval = Duration.ofMillis(DEFAULT_BROADCAST_INTERVAL);
  private int failureThreshold = DEFAULT_PHI_FAILURE_THRESHOLD;
  private Duration failureTimeout = Duration.ofMillis(DEFAULT_FAILURE_TIMEOUT);

  @Override
  public NodeDiscoveryProvider.Type getType() {
    return MulticastDiscoveryProvider.TYPE;
  }

  /**
   * Returns the broadcast interval.
   *
   * @return the broadcast interval
   */
  public Duration getBroadcastInterval() {
    return broadcastInterval;
  }

  /**
   * Sets the broadcast interval.
   *
   * @param broadcastInterval the broadcast interval
   * @return the group membership configuration
   */
  public MulticastDiscoveryConfig setBroadcastInterval(Duration broadcastInterval) {
    this.broadcastInterval = checkNotNull(broadcastInterval);
    return this;
  }

  /**
   * Returns the failure detector threshold.
   *
   * @return the failure detector threshold
   */
  @Deprecated
  public int getFailureThreshold() {
    return failureThreshold;
  }

  /**
   * Sets the failure detector threshold.
   *
   * @param failureThreshold the failure detector threshold
   * @return the group membership configuration
   */
  @Deprecated
  public MulticastDiscoveryConfig setFailureThreshold(int failureThreshold) {
    this.failureThreshold = failureThreshold;
    return this;
  }

  /**
   * Returns the base failure timeout.
   *
   * @return the base failure timeout
   */
  public Duration getFailureTimeout() {
    return failureTimeout;
  }

  /**
   * Sets the base failure timeout.
   *
   * @param failureTimeout the base failure timeout
   * @return the group membership configuration
   */
  public MulticastDiscoveryConfig setFailureTimeout(Duration failureTimeout) {
    this.failureTimeout = checkNotNull(failureTimeout);
    return this;
  }
}
