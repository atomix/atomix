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
package io.atomix.core.map;

import io.atomix.primitive.PrimitiveType;

/**
 * Distributed sorted map configuration.
 */
public class DistributedSortedMapConfig extends MapConfig<DistributedSortedMapConfig> {
  private boolean nullValues = false;

  @Override
  public PrimitiveType getType() {
    return DistributedSortedMapType.instance();
  }

  /**
   * Enables null values in the map.
   *
   * @return the map configuration
   */
  public DistributedSortedMapConfig setNullValues() {
    return setNullValues(true);
  }

  /**
   * Enables null values in the map.
   *
   * @param nullValues whether null values are allowed
   * @return the map configuration
   */
  public DistributedSortedMapConfig setNullValues(boolean nullValues) {
    this.nullValues = nullValues;
    return this;
  }

  /**
   * Returns whether null values are supported by the map.
   *
   * @return {@code true} if null values are supported; {@code false} otherwise
   */
  public boolean isNullValues() {
    return nullValues;
  }
}
