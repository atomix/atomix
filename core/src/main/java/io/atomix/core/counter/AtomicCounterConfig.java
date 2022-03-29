// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter;

import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Atomic counter configuration.
 */
public class AtomicCounterConfig extends PrimitiveConfig<AtomicCounterConfig> {
  @Override
  public PrimitiveType getType() {
    return AtomicCounterType.instance();
  }
}
