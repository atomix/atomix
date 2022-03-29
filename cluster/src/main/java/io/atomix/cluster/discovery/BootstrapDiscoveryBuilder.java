// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.discovery;

import io.atomix.cluster.Node;
import io.atomix.utils.net.Address;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Bootstrap discovery builder.
 */
public class BootstrapDiscoveryBuilder extends NodeDiscoveryBuilder {
  private final BootstrapDiscoveryConfig config = new BootstrapDiscoveryConfig();

  /**
   * Sets the bootstrap nodes.
   *
   * @param nodes the bootstrap nodes
   * @return the location provider builder
   */
  public BootstrapDiscoveryBuilder withNodes(Address... nodes) {
    return withNodes(Stream.of(nodes)
        .map(address -> Node.builder()
            .withAddress(address)
            .build())
        .collect(Collectors.toSet()));
  }

  /**
   * Sets the bootstrap nodes.
   *
   * @param nodes the bootstrap nodes
   * @return the location provider builder
   */
  public BootstrapDiscoveryBuilder withNodes(Node... nodes) {
    return withNodes(Arrays.asList(nodes));
  }

  /**
   * Sets the bootstrap nodes.
   *
   * @param locations the bootstrap member locations
   * @return the location provider builder
   */
  public BootstrapDiscoveryBuilder withNodes(Collection<Node> locations) {
    config.setNodes(locations.stream().map(Node::config).collect(Collectors.toList()));
    return this;
  }

  /**
   * Sets the failure detection heartbeat interval.
   *
   * @param heartbeatInterval the failure detection heartbeat interval
   * @return the location provider builder
   */
  public BootstrapDiscoveryBuilder withHeartbeatInterval(Duration heartbeatInterval) {
    config.setHeartbeatInterval(heartbeatInterval);
    return this;
  }

  /**
   * Sets the phi accrual failure threshold.
   *
   * @param failureThreshold the phi accrual failure threshold
   * @return the location provider builder
   */
  public BootstrapDiscoveryBuilder withFailureThreshold(int failureThreshold) {
    config.setFailureThreshold(failureThreshold);
    return this;
  }

  /**
   * Sets the failure timeout to use prior to phi failure detectors being populated.
   *
   * @param failureTimeout the failure timeout
   * @return the location provider builder
   */
  public BootstrapDiscoveryBuilder withFailureTimeout(Duration failureTimeout) {
    config.setFailureTimeout(failureTimeout);
    return this;
  }

  @Override
  public NodeDiscoveryProvider build() {
    return new BootstrapDiscoveryProvider(config);
  }
}
