// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value;

import io.atomix.primitive.PrimitiveType;

/**
 * Atomic value configuration.
 */
public class AtomicValueConfig extends ValueConfig<AtomicValueConfig> {
  @Override
  public PrimitiveType getType() {
    return AtomicValueType.instance();
  }
}
