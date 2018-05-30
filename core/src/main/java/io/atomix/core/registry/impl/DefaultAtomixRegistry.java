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

import io.atomix.core.profile.ProfileTypeRegistry;
import io.atomix.core.profile.impl.DefaultProfileTypeRegistry;
import io.atomix.core.registry.AtomixRegistry;
import io.atomix.core.registry.RegistryConfig;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.impl.DefaultPrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.impl.DefaultPartitionGroupTypeRegistry;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.protocol.impl.DefaultPrimitiveProtocolTypeRegistry;

/**
 * Default registry service.
 */
public class DefaultAtomixRegistry implements AtomixRegistry {
  private final PartitionGroupTypeRegistry partitionGroupTypes;
  private final PrimitiveTypeRegistry primitiveTypes;
  private final PrimitiveProtocolTypeRegistry protocolTypes;
  private final ProfileTypeRegistry profileTypes;

  public DefaultAtomixRegistry(RegistryConfig config) {
    this.partitionGroupTypes = new DefaultPartitionGroupTypeRegistry(config.getPartitionGroupTypes().values());
    this.primitiveTypes = new DefaultPrimitiveTypeRegistry(config.getPrimitiveTypes().values());
    this.protocolTypes = new DefaultPrimitiveProtocolTypeRegistry(config.getProtocolTypes().values());
    this.profileTypes = new DefaultProfileTypeRegistry(config.getProfileTypes().values());
  }

  @Override
  public PartitionGroupTypeRegistry partitionGroupTypes() {
    return partitionGroupTypes;
  }

  @Override
  public PrimitiveTypeRegistry primitiveTypes() {
    return primitiveTypes;
  }

  @Override
  public PrimitiveProtocolTypeRegistry protocolTypes() {
    return protocolTypes;
  }

  @Override
  public ProfileTypeRegistry profileTypes() {
    return profileTypes;
  }
}
