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
package io.atomix.core;

import io.atomix.cluster.AtomixClusterBuilder;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.core.profile.Profile;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.utils.net.Address;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix builder.
 */
public class AtomixBuilder extends AtomixClusterBuilder {
  private final AtomixConfig config;
  private final AtomixRegistry registry;

  protected AtomixBuilder(AtomixConfig config, AtomixRegistry registry) {
    super(config.getClusterConfig());
    this.config = checkNotNull(config);
    this.registry = checkNotNull(registry);
  }

  /**
   * Enables the shutdown hook.
   *
   * @return the Atomix builder
   */
  public AtomixBuilder withShutdownHookEnabled() {
    return withShutdownHook(true);
  }

  /**
   * Enables the shutdown hook.
   *
   * @param enabled if <code>true</code> a shutdown hook will be registered
   * @return the Atomix builder
   */
  public AtomixBuilder withShutdownHook(boolean enabled) {
    config.setEnableShutdownHook(enabled);
    return this;
  }

  /**
   * Sets the Atomix profiles.
   *
   * @param profiles the profiles
   * @return the Atomix builder
   */
  public AtomixBuilder withProfiles(Profile... profiles) {
    return withProfiles(Arrays.asList(checkNotNull(profiles)));
  }

  /**
   * Sets the Atomix profiles.
   *
   * @param profiles the profiles
   * @return the Atomix builder
   */
  public AtomixBuilder withProfiles(Collection<Profile> profiles) {
    profiles.forEach(profile -> config.addProfile(profile.config()));
    return this;
  }

  /**
   * Adds an Atomix profile.
   *
   * @param profile the profile to add
   * @return the Atomix builder
   */
  public AtomixBuilder addProfile(Profile profile) {
    config.addProfile(profile.config());
    return this;
  }

  /**
   * Sets the system management partition group.
   *
   * @param systemManagementGroup the system management partition group
   * @return the Atomix builder
   */
  public AtomixBuilder withManagementGroup(ManagedPartitionGroup systemManagementGroup) {
    config.setManagementGroup(systemManagementGroup.config());
    return this;
  }

  /**
   * Sets the partition groups.
   *
   * @param partitionGroups the partition groups
   * @return the Atomix builder
   * @throws NullPointerException if the partition groups are null
   */
  public AtomixBuilder withPartitionGroups(ManagedPartitionGroup... partitionGroups) {
    return withPartitionGroups(Arrays.asList(checkNotNull(partitionGroups, "partitionGroups cannot be null")));
  }

  /**
   * Sets the partition groups.
   *
   * @param partitionGroups the partition groups
   * @return the Atomix builder
   * @throws NullPointerException if the partition groups are null
   */
  public AtomixBuilder withPartitionGroups(Collection<ManagedPartitionGroup> partitionGroups) {
    partitionGroups.forEach(group -> config.addPartitionGroup(group.config()));
    return this;
  }

  /**
   * Adds a partition group.
   *
   * @param partitionGroup the partition group to add
   * @return the Atomix builder
   * @throws NullPointerException if the partition group is null
   */
  public AtomixBuilder addPartitionGroup(ManagedPartitionGroup partitionGroup) {
    config.addPartitionGroup(partitionGroup.config());
    return this;
  }

  @Override
  public AtomixBuilder withClusterId(String clusterId) {
    super.withClusterId(clusterId);
    return this;
  }

  @Override
  public AtomixBuilder withMemberId(String localMemberId) {
    super.withMemberId(localMemberId);
    return this;
  }

  @Override
  public AtomixBuilder withMemberId(MemberId localMemberId) {
    super.withMemberId(localMemberId);
    return this;
  }

  @Override
  public AtomixBuilder withAddress(String address) {
    super.withAddress(address);
    return this;
  }

  @Override
  public AtomixBuilder withAddress(String host, int port) {
    super.withAddress(host, port);
    return this;
  }

  @Override
  public AtomixBuilder withAddress(int port) {
    super.withAddress(port);
    return this;
  }

  @Override
  public AtomixBuilder withAddress(Address address) {
    super.withAddress(address);
    return this;
  }

  @Override
  public AtomixBuilder withZone(String zone) {
    super.withZone(zone);
    return this;
  }

  @Override
  public AtomixBuilder withRack(String rack) {
    super.withRack(rack);
    return this;
  }

  @Override
  public AtomixBuilder withHost(String host) {
    super.withHost(host);
    return this;
  }

  @Override
  public AtomixBuilder withProperties(Properties properties) {
    super.withProperties(properties);
    return this;
  }

  @Override
  public AtomixBuilder withProperty(String key, String value) {
    super.withProperty(key, value);
    return this;
  }

  @Override
  public AtomixBuilder withMulticastEnabled() {
    super.withMulticastEnabled();
    return this;
  }

  @Override
  public AtomixBuilder withMulticastEnabled(boolean multicastEnabled) {
    super.withMulticastEnabled(multicastEnabled);
    return this;
  }

  @Override
  public AtomixBuilder withMulticastAddress(Address address) {
    super.withMulticastAddress(address);
    return this;
  }

  @Override
  public AtomixBuilder withMembershipProvider(NodeDiscoveryProvider locationProvider) {
    super.withMembershipProvider(locationProvider);
    return this;
  }

  @Override
  public AtomixBuilder setBroadcastInterval(Duration interval) {
    super.setBroadcastInterval(interval);
    return this;
  }

  @Override
  public AtomixBuilder setReachabilityThreshold(int threshold) {
    super.setReachabilityThreshold(threshold);
    return this;
  }

  @Override
  public AtomixBuilder withReachabilityTimeout(Duration timeout) {
    super.withReachabilityTimeout(timeout);
    return this;
  }

  /**
   * Builds a new Atomix instance.
   *
   * @return a new Atomix instance
   */
  @Override
  public Atomix build() {
    return new Atomix(config, registry);
  }
}
