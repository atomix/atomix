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

/**
 * Gossip based group membership protocol builder.
 */
public class PhiMembershipProtocolBuilder extends GroupMembershipProtocolBuilder {
  private final PhiMembershipProtocolConfig config = new PhiMembershipProtocolConfig();

  /**
   * Sets the failure detection heartbeat interval.
   *
   * @param heartbeatInterval the failure detection heartbeat interval
   * @return the location provider builder
   */
  public PhiMembershipProtocolBuilder withHeartbeatInterval(Duration heartbeatInterval) {
    config.setHeartbeatInterval(heartbeatInterval);
    return this;
  }

  /**
   * Sets the phi accrual failure threshold.
   *
   * @param failureThreshold the phi accrual failure threshold
   * @return the location provider builder
   */
  public PhiMembershipProtocolBuilder withFailureThreshold(int failureThreshold) {
    config.setFailureThreshold(failureThreshold);
    return this;
  }

  /**
   * Sets the failure timeout to use prior to phi failure detectors being populated.
   *
   * @param failureTimeout the failure timeout
   * @return the location provider builder
   */
  public PhiMembershipProtocolBuilder withFailureTimeout(Duration failureTimeout) {
    config.setFailureTimeout(failureTimeout);
    return this;
  }

  @Override
  public GroupMembershipProtocol build() {
    return new PhiMembershipProtocol(config);
  }
}
