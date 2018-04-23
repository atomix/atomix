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
package io.atomix.primitive.partition;

import io.atomix.utils.Services;
import io.atomix.utils.config.ConfigurationException;

import java.util.Collection;

/**
 * Partition groups.
 */
public class PartitionGroups {

  /**
   * Creates a new protocol instance from the given configuration.
   *
   * @param config the configuration from which to create the protocol instance
   * @return the protocol instance for the given configuration
   */
  @SuppressWarnings("unchecked")
  public static ManagedPartitionGroup createGroup(PartitionGroupConfig config) {
    for (PartitionGroupFactory factory : Services.loadAll(PartitionGroupFactory.class)) {
      if (factory.configClass().isAssignableFrom(config.getClass())) {
        return factory.createGroup(config);
      }
    }
    throw new ConfigurationException("Unknown partition group configuration type: " + config.getClass().getSimpleName());
  }

  /**
   * Returns the partition group factory for the given type.
   *
   * @param type the type for which to return the factory
   * @return the partition group factory for the given type
   */
  public static PartitionGroupFactory getGroupFactory(String type) {
    for (PartitionGroupFactory factory : Services.loadAll(PartitionGroupFactory.class)) {
      if (factory.type().name().equals(type)) {
        return factory;
      }
    }
    throw new ConfigurationException("Unknown partition group type: " + type);
  }

  /**
   * Returns the partition group factories.
   *
   * @return the partition group factories
   */
  public static Collection<PartitionGroupFactory> getGroupFactories() {
    return Services.loadAll(PartitionGroupFactory.class);
  }

  private PartitionGroups() {
  }
}
