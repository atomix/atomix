// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.primitive.PrimitiveType;

/**
 * Consistent tree-map configuration.
 */
public class AtomicNavigableMapConfig extends MapConfig<AtomicNavigableMapConfig> {
  @Override
  public PrimitiveType getType() {
    return AtomicNavigableMapType.instance();
  }
}
