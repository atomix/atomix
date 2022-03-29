// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;

/**
 * Distributed counter configuration.
 */
public class DistributedCounterConfig extends PrimitiveConfig<DistributedCounterConfig> {
  @Override
  public PrimitiveType getType() {
    return DistributedCounterType.instance();
  }
}
