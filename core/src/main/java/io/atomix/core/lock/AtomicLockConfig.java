// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;

/**
 * Distributed lock configuration.
 */
public class AtomicLockConfig extends PrimitiveConfig<AtomicLockConfig> {
  @Override
  public PrimitiveType getType() {
    return AtomicLockType.instance();
  }
}
