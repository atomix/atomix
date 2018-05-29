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
package io.atomix.core.registry;

import io.atomix.core.profile.ProfileType;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionGroupType;
import io.atomix.primitive.protocol.PrimitiveProtocolType;

import java.util.HashMap;
import java.util.Map;

/**
 * Atomix object registry.
 */
public class RegistryConfig {
  private Map<String, PrimitiveType> primitiveTypes = new HashMap<>();
  private Map<String, PartitionGroupType> partitionGroupTypes = new HashMap<>();
  private Map<String, PrimitiveProtocolType> protocolTypes = new HashMap<>();
  private Map<String, ProfileType> profileTypes = new HashMap<>();

  /**
   * Returns the primitive types.
   *
   * @return the primitive types
   */
  public Map<String, PrimitiveType> getPrimitiveTypes() {
    return primitiveTypes;
  }

  /**
   * Sets the primitive types.
   *
   * @param primitives the primitive types
   * @return the registry
   */
  public RegistryConfig setPrimitiveTypes(Map<String, PrimitiveType> primitives) {
    this.primitiveTypes = primitives;
    return this;
  }

  /**
   * Adds a primitive to the registry.
   *
   * @param primitive the primitive to add
   * @return the registry
   */
  public RegistryConfig addPrimitiveType(PrimitiveType primitive) {
    primitiveTypes.put(primitive.name(), primitive);
    return this;
  }

  /**
   * Returns the partition groups.
   *
   * @return the partition groups
   */
  public Map<String, PartitionGroupType> getPartitionGroupTypes() {
    return partitionGroupTypes;
  }

  /**
   * Sets the partition group types.
   *
   * @param partitionGroups the partition group types
   * @return the registry
   */
  public RegistryConfig setPartitionGroupTypes(Map<String, PartitionGroupType> partitionGroups) {
    this.partitionGroupTypes = partitionGroups;
    return this;
  }

  /**
   * Adds a partition group to the registry.
   *
   * @param partitionGroup the group to add
   * @return the registry
   */
  public RegistryConfig addPartitionGroupType(PartitionGroupType partitionGroup) {
    partitionGroupTypes.put(partitionGroup.name(), partitionGroup);
    return this;
  }

  /**
   * Returns the primitive protocols.
   *
   * @return the primitive protocols
   */
  public Map<String, PrimitiveProtocolType> getProtocolTypes() {
    return protocolTypes;
  }

  /**
   * Sets the protocol types.
   *
   * @param protocols the protocol types
   * @return the registry
   */
  public RegistryConfig setProtocolTypes(Map<String, PrimitiveProtocolType> protocols) {
    this.protocolTypes = protocols;
    return this;
  }

  /**
   * Adds a protocol type to the registry.
   *
   * @param protocol the protocol to add
   * @return the registry
   */
  public RegistryConfig addProtocolType(PrimitiveProtocolType protocol) {
    protocolTypes.put(protocol.name(), protocol);
    return this;
  }

  /**
   * Returns the profile types.
   *
   * @return the profile types
   */
  public Map<String, ProfileType> getProfileTypes() {
    return profileTypes;
  }

  /**
   * Sets the profile types.
   *
   * @param profiles the profile types
   * @return the registry
   */
  public RegistryConfig setProfileTypes(Map<String, ProfileType> profiles) {
    this.profileTypes = profiles;
    return this;
  }

  /**
   * Adds a profile type to the registry.
   *
   * @param profile the profile to add
   * @return the registry
   */
  public RegistryConfig addProfileType(ProfileType profile) {
    profileTypes.put(profile.name(), profile);
    return this;
  }
}
