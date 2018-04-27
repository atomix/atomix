/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster;

import io.atomix.utils.config.Config;

/**
 * Group membership protocol configuration.
 */
public class GroupMembershipConfig implements Config {
  private static final int DEFAULT_HEARTBEAT_INTERVAL = 100;
  private static final int DEFAULT_FAILURE_TIMEOUT = 10000;
  private static final int DEFAULT_PHI_FAILURE_THRESHOLD = 10;

  private int heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
  private int phiFailureThreshold = DEFAULT_PHI_FAILURE_THRESHOLD;
  private int failureTimeout = DEFAULT_FAILURE_TIMEOUT;

  /**
   * Returns the failure detector heartbeat interval.
   *
   * @return the failure detector heartbeat interval
   */
  public int getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Sets the failure detector heartbeat interval.
   *
   * @param heartbeatInterval the failure detector heartbeat interval
   * @return the group membership configuration
   */
  public GroupMembershipConfig setHeartbeatInterval(int heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
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
  public GroupMembershipConfig setPhiFailureThreshold(int phiFailureThreshold) {
    this.phiFailureThreshold = phiFailureThreshold;
    return this;
  }

  /**
   * Returns the base failure timeout.
   *
   * @return the base failure timeout
   */
  public int getFailureTimeout() {
    return failureTimeout;
  }

  /**
   * Sets the base failure timeout.
   *
   * @param failureTimeout the base failure timeout
   * @return the group membership configuration
   */
  public GroupMembershipConfig setFailureTimeout(int failureTimeout) {
    this.failureTimeout = failureTimeout;
    return this;
  }
}
