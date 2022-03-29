// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value;

import io.atomix.primitive.PrimitiveType;

/**
 * Distributed value configuration.
 */
public class DistributedValueConfig extends ValueConfig<DistributedValueConfig> {
  @Override
  public PrimitiveType getType() {
    return DistributedValueType.instance();
  }
}
