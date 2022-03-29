// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.core.set.DistributedSetType;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Transactional set configuration.
 */
public class TransactionalSetConfig extends PrimitiveConfig {
  @Override
  public PrimitiveType getType() {
    return DistributedSetType.instance();
  }
}
