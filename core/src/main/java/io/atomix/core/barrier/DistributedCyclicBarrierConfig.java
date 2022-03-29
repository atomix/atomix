// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.barrier;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;

/**
 * Distributed cyclic barrier configuration.
 */
public class DistributedCyclicBarrierConfig extends PrimitiveConfig<DistributedCyclicBarrierConfig> {
  @Override
  public PrimitiveType getType() {
    return DistributedCyclicBarrierType.instance();
  }
}
