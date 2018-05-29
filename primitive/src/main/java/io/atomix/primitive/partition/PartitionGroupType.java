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

import io.atomix.utils.AbstractNamed;
import io.atomix.utils.Type;
import io.atomix.utils.config.ConfigurationException;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Partition group type configuration.
 */
public class PartitionGroupType extends AbstractNamed implements Type {
  private Class<? extends PartitionGroupConfig> configClass;
  private Class<? extends PartitionGroupFactory> factoryClass;

  /**
   * Returns the partition group configuration class.
   *
   * @return the partition group configuration class
   */
  public Class<? extends PartitionGroupConfig> configClass() {
    return configClass;
  }

  /**
   * Returns the partition group factory class.
   *
   * @return the partition group factory class
   */
  public Class<? extends PartitionGroupFactory> factoryClass() {
    return factoryClass;
  }

  /**
   * Creates a new partition group instance.
   *
   * @param config the partition group configuration
   * @return the partition group
   */
  @SuppressWarnings("unchecked")
  public ManagedPartitionGroup newGroup(PartitionGroupConfig config) {
    if (factoryClass == null) {
      throw new ConfigurationException("No partition group factory class configured for group " + name());
    }
    try {
      return factoryClass.newInstance().createGroup(config);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to instantiate partition group factory", e);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(name());
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof PartitionGroupType && Objects.equals(((PartitionGroupType) object).name(), name());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
