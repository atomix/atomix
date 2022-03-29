// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.protocol;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Gossip group membership protocol configuration.
 */
public class HeartbeatMembershipProtocolConfig extends GroupMembershipProtocolConfig {
  private static final int DEFAULT_HEARTBEAT_INTERVAL = 1000;
  private static final int DEFAULT_FAILURE_TIMEOUT = 10000;
  private static final int DEFAULT_PHI_FAILURE_THRESHOLD = 10;

  private Duration heartbeatInterval = Duration.ofMillis(DEFAULT_HEARTBEAT_INTERVAL);
  private int phiFailureThreshold = DEFAULT_PHI_FAILURE_THRESHOLD;
  private Duration failureTimeout = Duration.ofMillis(DEFAULT_FAILURE_TIMEOUT);

  @Override
  public GroupMembershipProtocol.Type getType() {
    return HeartbeatMembershipProtocol.TYPE;
  }

  /**
   * Returns the heartbeat interval.
   *
   * @return the heartbeat interval
   */
  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Sets the heartbeat interval.
   *
   * @param heartbeatInterval the heartbeat interval
   * @return the group membership configuration
   */
  public HeartbeatMembershipProtocolConfig setHeartbeatInterval(Duration heartbeatInterval) {
    this.heartbeatInterval = checkNotNull(heartbeatInterval);
    return this;
  }

  /**
   * Returns the failure detector threshold.
   *
   * @return the failure detector threshold
   */
  public int getPhiFailureThreshold() {
    return phiFailureThreshold;
  }

  /**
   * Sets the failure detector threshold.
   *
   * @param phiFailureThreshold the failure detector threshold
   * @return the group membership configuration
   */
  public HeartbeatMembershipProtocolConfig setPhiFailureThreshold(int phiFailureThreshold) {
    this.phiFailureThreshold = phiFailureThreshold;
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
  public HeartbeatMembershipProtocolConfig setFailureTimeout(Duration failureTimeout) {
    this.failureTimeout = checkNotNull(failureTimeout);
    return this;
  }
}
