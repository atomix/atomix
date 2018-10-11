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
package io.atomix.core.registry.impl;

import com.google.common.collect.Maps;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.core.profile.Profile;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.util.Map;

/**
 * Config file registry configuration.
 */
public class ConfigFileRegistryConfig {
  private Map<String, Class<? extends NodeDiscoveryProvider.Type<?>>> discoveryProviderTypes = Maps.newHashMap();
  private Map<String, Class<? extends Profile.Type<?>>> profileTypes = Maps.newHashMap();
  private Map<String, Class<? extends PrimitiveType<?, ?, ?>>> primitiveTypes = Maps.newHashMap();
  private Map<String, Class<? extends PrimitiveProtocol.Type<?>>> protocolTypes = Maps.newConcurrentMap();
  private Map<String, Class<? extends PartitionGroup.Type<?>>> partitionGroupTypes = Maps.newHashMap();

  public Map<String, Class<? extends NodeDiscoveryProvider.Type<?>>> getDiscoveryProviderTypes() {
    return discoveryProviderTypes;
  }

  public void setDiscoveryProviderTypes(Map<String, Class<? extends NodeDiscoveryProvider.Type<?>>> discoveryProviderTypes) {
    this.discoveryProviderTypes = discoveryProviderTypes;
  }

  public Map<String, Class<? extends Profile.Type<?>>> getProfileTypes() {
    return profileTypes;
  }

  public void setProfileTypes(Map<String, Class<? extends Profile.Type<?>>> profileTypes) {
    this.profileTypes = profileTypes;
  }

  public Map<String, Class<? extends PrimitiveType<?, ?, ?>>> getPrimitiveTypes() {
    return primitiveTypes;
  }

  public void setPrimitiveTypes(Map<String, Class<? extends PrimitiveType<?, ?, ?>>> primitiveTypes) {
    this.primitiveTypes = primitiveTypes;
  }

  public Map<String, Class<? extends PrimitiveProtocol.Type<?>>> getProtocolTypes() {
    return protocolTypes;
  }

  public void setProtocolTypes(Map<String, Class<? extends PrimitiveProtocol.Type<?>>> protocolTypes) {
    this.protocolTypes = protocolTypes;
  }

  public Map<String, Class<? extends PartitionGroup.Type<?>>> getPartitionGroupTypes() {
    return partitionGroupTypes;
  }

  public void setPartitionGroupTypes(Map<String, Class<? extends PartitionGroup.Type<?>>> partitionGroupTypes) {
    this.partitionGroupTypes = partitionGroupTypes;
  }
}
