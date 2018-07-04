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
 * Cluster membership configuration.
 */
public class MembershipConfig implements Config {
  private static final int DEFAULT_BROADCAST_INTERVAL = 100;
  private static final int DEFAULT_REACHABILITY_TIMEOUT = 10000;
  private static final int DEFAULT_REACHABILITY_THRESHOLD = 10;

  private int broadcastInterval = DEFAULT_BROADCAST_INTERVAL;
  private int reachabilityThreshold = DEFAULT_REACHABILITY_THRESHOLD;
  private int reachabilityTimeout = DEFAULT_REACHABILITY_TIMEOUT;

  /**
   * Returns the reachability broadcast interval.
   *
   * @return the reachability broadcast interval
   */
  public int getBroadcastInterval() {
    return broadcastInterval;
  }

  /**
   * Sets the reachability broadcast interval.
   *
   * @param broadcastInterval the reachability broadcast interval
   * @return the membership configuration
   */
  public MembershipConfig setBroadcastInterval(int broadcastInterval) {
    this.broadcastInterval = broadcastInterval;
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
  public int getReachabilityTimeout() {
    return reachabilityTimeout;
  }

  /**
   * Sets the reachability failure timeout.
   *
   * @param reachabilityTimeout the reachability failure timeout
   * @return the membership configuration
   */
  public MembershipConfig setReachabilityTimeout(int reachabilityTimeout) {
    this.reachabilityTimeout = reachabilityTimeout;
    return this;
  }
}
