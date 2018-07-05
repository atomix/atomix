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

import io.atomix.utils.Builder;
import io.atomix.utils.net.Address;

import java.time.Duration;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix cluster builder.
 */
public class AtomixClusterBuilder implements Builder<AtomixCluster> {
  protected final ClusterConfig config;

  protected AtomixClusterBuilder() {
    this(new ClusterConfig());
  }

  protected AtomixClusterBuilder(ClusterConfig config) {
    this.config = checkNotNull(config);
  }

  /**
   * Sets the cluster identifier.
   *
   * @param clusterId the cluster identifier
   * @return the cluster builder
   */
  public AtomixClusterBuilder withClusterId(String clusterId) {
    config.setClusterId(clusterId);
    return this;
  }

  /**
   * Sets the local member identifier.
   *
   * @param localMemberId the local member identifier
   * @return the cluster builder
   */
  public AtomixClusterBuilder withMemberId(String localMemberId) {
    config.getNodeConfig().setId(localMemberId);
    return this;
  }

  /**
   * Sets the local member identifier.
   *
   * @param localMemberId the local member identifier
   * @return the cluster builder
   */
  public AtomixClusterBuilder withMemberId(MemberId localMemberId) {
    config.getNodeConfig().setId(localMemberId);
    return this;
  }

  /**
   * Sets the member address.
   *
   * @param address a host:port tuple
   * @return the member builder
   * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
   */
  public AtomixClusterBuilder withAddress(String address) {
    return withAddress(Address.from(address));
  }

  /**
   * Sets the member host/port.
   *
   * @param host the host name
   * @param port the port number
   * @return the member builder
   * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
   */
  public AtomixClusterBuilder withAddress(String host, int port) {
    return withAddress(Address.from(host, port));
  }

  /**
   * Sets the member address using local host.
   *
   * @param port the port number
   * @return the member builder
   * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
   */
  public AtomixClusterBuilder withAddress(int port) {
    return withAddress(Address.from(port));
  }

  /**
   * Sets the member address.
   *
   * @param address the member address
   * @return the member builder
   */
  public AtomixClusterBuilder withAddress(Address address) {
    config.getNodeConfig().setAddress(address);
    return this;
  }

  /**
   * Sets the zone to which the member belongs.
   *
   * @param zone the zone to which the member belongs
   * @return the member builder
   */
  public AtomixClusterBuilder withZone(String zone) {
    config.getNodeConfig().setZone(zone);
    return this;
  }

  /**
   * Sets the rack to which the member belongs.
   *
   * @param rack the rack to which the member belongs
   * @return the member builder
   */
  public AtomixClusterBuilder withRack(String rack) {
    config.getNodeConfig().setRack(rack);
    return this;
  }

  /**
   * Sets the host to which the member belongs.
   *
   * @param host the host to which the member belongs
   * @return the member builder
   */
  public AtomixClusterBuilder withHost(String host) {
    config.getNodeConfig().setHost(host);
    return this;
  }

  /**
   * Sets the member properties.
   *
   * @param properties the member properties
   * @return the member builder
   * @throws NullPointerException if the properties are null
   */
  public AtomixClusterBuilder withProperties(Properties properties) {
    config.getNodeConfig().setProperties(properties);
    return this;
  }

  /**
   * Sets a property of the member.
   *
   * @param key   the property key to set
   * @param value the property value to set
   * @return the member builder
   * @throws NullPointerException if the property is null
   */
  public AtomixClusterBuilder withProperty(String key, String value) {
    config.getNodeConfig().setProperty(key, value);
    return this;
  }

  /**
   * Enables multicast node discovery.
   *
   * @return the Atomix builder
   */
  public AtomixClusterBuilder withMulticastEnabled() {
    return withMulticastEnabled(true);
  }

  /**
   * Sets whether multicast node discovery is enabled.
   *
   * @param multicastEnabled whether to enable multicast node discovery
   * @return the Atomix builder
   */
  public AtomixClusterBuilder withMulticastEnabled(boolean multicastEnabled) {
    config.getMulticastConfig().setEnabled(multicastEnabled);
    return this;
  }

  /**
   * Sets the multicast address.
   *
   * @param address the multicast address
   * @return the Atomix builder
   */
  public AtomixClusterBuilder withMulticastAddress(Address address) {
    config.getMulticastConfig().setGroup(address.address());
    config.getMulticastConfig().setPort(address.port());
    return this;
  }

  /**
   * Sets the reachability broadcast interval.
   *
   * @param interval the reachability broadcast interval
   * @return the Atomix builder
   */
  public AtomixClusterBuilder setBroadcastInterval(Duration interval) {
    config.getMembershipConfig().setBroadcastInterval(interval);
    return this;
  }

  /**
   * Sets the reachability failure detection threshold.
   *
   * @param threshold the reachability failure detection threshold
   * @return the Atomix builder
   */
  public AtomixClusterBuilder setReachabilityThreshold(int threshold) {
    config.getMembershipConfig().setReachabilityThreshold(threshold);
    return this;
  }

  /**
   * Sets the reachability failure timeout.
   *
   * @param timeout the reachability failure timeout
   * @return the Atomix builder
   */
  public AtomixClusterBuilder withReachabilityTimeout(Duration timeout) {
    config.getMembershipConfig().setReachabilityTimeout(timeout);
    return this;
  }

  /**
   * Sets the membership provider.
   *
   * @param locationProvider the membership provider
   * @return the Atomix cluster builder
   */
  public AtomixClusterBuilder withMembershipProvider(NodeDiscoveryProvider locationProvider) {
    config.setDiscoveryConfig(locationProvider.config());
    return this;
  }

  @Override
  public AtomixCluster build() {
    return new AtomixCluster(config);
  }
}
