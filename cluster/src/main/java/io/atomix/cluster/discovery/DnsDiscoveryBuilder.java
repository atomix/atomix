// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.discovery;

import java.time.Duration;

/**
 * DNS discovery builder.
 */
public class DnsDiscoveryBuilder extends NodeDiscoveryBuilder {
  private final DnsDiscoveryConfig config = new DnsDiscoveryConfig();

  /**
   * Sets the DNS service name.
   *
   * @param service the DNS service name
   * @return the DNS discovery builder
   */
  public DnsDiscoveryBuilder withService(String service) {
    config.setService(service);
    return this;
  }

  /**
   * Sets the DNS resolution interval.
   *
   * @param resolutionInterval the DNS resolution interval
   * @return the DNS configuration
   */
  public DnsDiscoveryBuilder withResolutionInterval(Duration resolutionInterval) {
    config.setResolutionInterval(resolutionInterval);
    return this;
  }

  @Override
  public NodeDiscoveryProvider build() {
    return new DnsDiscoveryProvider(config);
  }
}
