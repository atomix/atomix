// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.protocol;

import java.time.Duration;

/**
 * Gossip based group membership protocol builder.
 */
public class HeartbeatMembershipProtocolBuilder extends GroupMembershipProtocolBuilder {
  private final HeartbeatMembershipProtocolConfig config = new HeartbeatMembershipProtocolConfig();

  /**
   * Sets the failure detection heartbeat interval.
   *
   * @param heartbeatInterval the failure detection heartbeat interval
   * @return the location provider builder
   */
  public HeartbeatMembershipProtocolBuilder withHeartbeatInterval(Duration heartbeatInterval) {
    config.setHeartbeatInterval(heartbeatInterval);
    return this;
  }

  /**
   * Sets the phi accrual failure threshold.
   *
   * @param failureThreshold the phi accrual failure threshold
   * @return the location provider builder
   */
  public HeartbeatMembershipProtocolBuilder withFailureThreshold(int failureThreshold) {
    config.setPhiFailureThreshold(failureThreshold);
    return this;
  }

  /**
   * Sets the failure timeout to use prior to phi failure detectors being populated.
   *
   * @param failureTimeout the failure timeout
   * @return the location provider builder
   */
  public HeartbeatMembershipProtocolBuilder withFailureTimeout(Duration failureTimeout) {
    config.setFailureTimeout(failureTimeout);
    return this;
  }

  @Override
  public GroupMembershipProtocol build() {
    return new HeartbeatMembershipProtocol(config);
  }
}
