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

import io.atomix.utils.Generics;

/**
 * Partition group factory.
 */
public interface PartitionGroupFactory<C extends PartitionGroupConfig<C>, P extends ManagedPartitionGroup> {

  /**
   * Returns the partition group protocol type.
   *
   * @return the partition group protocol type
   */
  PartitionGroup.Type type();

  /**
   * Returns the partition group configuration class.
   *
   * @return the partition group configuration class
   */
  @SuppressWarnings("unchecked")
  default Class<? extends PartitionGroupConfig> configClass() {
    return (Class<? extends PartitionGroupConfig>) Generics.getGenericInterfaceType(this, PartitionGroupFactory.class, 0);
  }

  /**
   * Returns the partition group class.
   *
   * @return the partition group class
   */
  @SuppressWarnings("unchecked")
  default Class<? extends ManagedPartitionGroup> groupClass() {
    return (Class<? extends ManagedPartitionGroup>) Generics.getGenericInterfaceType(this, PartitionGroupFactory.class, 1);
  }

  /**
   * Creates a new partition group.
   *
   * @param config the partition group configuration
   * @return the partition group
   */
  P createGroup(C config);

}
