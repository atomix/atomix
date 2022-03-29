// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.discovery;

import java.time.Duration;

/**
 * Multicast discovery provider builder.
 */
public class MulticastDiscoveryBuilder extends NodeDiscoveryBuilder {
  private final MulticastDiscoveryConfig config = new MulticastDiscoveryConfig();

  protected MulticastDiscoveryBuilder() {
  }

  /**
   * Sets the broadcast interval.
   *
   * @param broadcastInterval the broadcast interval
   * @return the location provider builder
   */
  public MulticastDiscoveryBuilder withBroadcastInterval(Duration broadcastInterval) {
    config.setBroadcastInterval(broadcastInterval);
    return this;
  }

  /**
   * Sets the phi accrual failure threshold.
   *
   * @param failureThreshold the phi accrual failure threshold
   * @return the location provider builder
   */
  public MulticastDiscoveryBuilder withFailureThreshold(int failureThreshold) {
    config.setFailureThreshold(failureThreshold);
    return this;
  }

  /**
   * Sets the failure timeout to use prior to phi failure detectors being populated.
   *
   * @param failureTimeout the failure timeout
   * @return the location provider builder
   */
  public MulticastDiscoveryBuilder withFailureTimeout(Duration failureTimeout) {
    config.setFailureTimeout(failureTimeout);
    return this;
  }

  @Override
  public NodeDiscoveryProvider build() {
    return new MulticastDiscoveryProvider(config);
  }
}
