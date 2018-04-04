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

import io.atomix.utils.Config;

/**
 * Partition group configuration.
 */
public abstract class PartitionGroupConfig<C extends PartitionGroupConfig<C>> implements Config {
  private String name;

  /**
   * Returns the partition group name.
   *
   * @return the partition group name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the partition group name.
   *
   * @param name the partition group name
   */
  @SuppressWarnings("unchecked")
  public C setName(String name) {
    this.name = name;
    return (C) this;
  }
}
