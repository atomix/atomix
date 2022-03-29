// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.discovery;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * DNS discovery configuration.
 */
public class DnsDiscoveryConfig extends NodeDiscoveryConfig {
  private String service;
  private Duration resolutionInterval = Duration.ofSeconds(15);

  @Override
  public NodeDiscoveryProvider.Type getType() {
    return DnsDiscoveryProvider.TYPE;
  }

  /**
   * Returns the discovery service.
   *
   * @return the DNS service to use for discovery
   */
  public String getService() {
    return service;
  }

  /**
   * Sets the DNS service name.
   *
   * @param service the DNS service name
   * @return the DNS configuration
   */
  public DnsDiscoveryConfig setService(String service) {
    this.service = checkNotNull(service);
    return this;
  }

  /**
   * Returns the DNS resolution interval.
   *
   * @return the DNS resolution interval
   */
  public Duration getResolutionInterval() {
    return resolutionInterval;
  }

  /**
   * Sets the DNS resolution interval.
   *
   * @param resolutionInterval the DNS resolution interval
   * @return the DNS configuration
   */
  public DnsDiscoveryConfig setResolutionInterval(Duration resolutionInterval) {
    this.resolutionInterval = checkNotNull(resolutionInterval, "resolutionInterval cannot be null");
    return this;
  }
}
