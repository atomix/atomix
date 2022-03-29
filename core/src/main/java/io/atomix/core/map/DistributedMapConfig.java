// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.primitive.PrimitiveType;

/**
 * Distributed map configuration.
 */
public class DistributedMapConfig extends MapConfig<DistributedMapConfig> {
  private boolean nullValues = false;

  @Override
  public PrimitiveType getType() {
    return DistributedMapType.instance();
  }

  /**
   * Enables null values in the map.
   *
   * @return the map configuration
   */
  public DistributedMapConfig setNullValues() {
    return setNullValues(true);
  }

  /**
   * Enables null values in the map.
   *
   * @param nullValues whether null values are allowed
   * @return the map configuration
   */
  public DistributedMapConfig setNullValues(boolean nullValues) {
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
