// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.primitive.PrimitiveType;

/**
 * Consistent sorted map configuration.
 */
public class AtomicSortedMapConfig extends MapConfig<AtomicSortedMapConfig> {
  @Override
  public PrimitiveType getType() {
    return AtomicSortedMapType.instance();
  }
}
