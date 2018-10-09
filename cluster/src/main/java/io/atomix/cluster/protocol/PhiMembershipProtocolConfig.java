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
package io.atomix.cluster.protocol;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Gossip group membership protocol configuration.
 */
public class PhiMembershipProtocolConfig extends GroupMembershipProtocolConfig {
  private static final int DEFAULT_HEARTBEAT_INTERVAL = 1000;
  private static final int DEFAULT_FAILURE_TIMEOUT = 10000;
  private static final int DEFAULT_PHI_FAILURE_THRESHOLD = 10;

  private Duration heartbeatInterval = Duration.ofMillis(DEFAULT_HEARTBEAT_INTERVAL);
  private int failureThreshold = DEFAULT_PHI_FAILURE_THRESHOLD;
  private Duration failureTimeout = Duration.ofMillis(DEFAULT_FAILURE_TIMEOUT);

  @Override
  public GroupMembershipProtocol.Type getType() {
    return PhiMembershipProtocol.TYPE;
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
  public PhiMembershipProtocolConfig setHeartbeatInterval(Duration heartbeatInterval) {
    this.heartbeatInterval = checkNotNull(heartbeatInterval);
    return this;
  }

  /**
   * Returns the failure detector threshold.
   *
   * @return the failure detector threshold
   */
  public int getFailureThreshold() {
    return failureThreshold;
  }

  /**
   * Sets the failure detector threshold.
   *
   * @param failureThreshold the failure detector threshold
   * @return the group membership configuration
   */
  public PhiMembershipProtocolConfig setFailureThreshold(int failureThreshold) {
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
  public PhiMembershipProtocolConfig setFailureTimeout(Duration failureTimeout) {
    this.failureTimeout = checkNotNull(failureTimeout);
    return this;
  }
}
