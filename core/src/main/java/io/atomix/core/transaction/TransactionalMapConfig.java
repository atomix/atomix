// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.core.map.AtomicMapType;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Transactional map configuration.
 */
public class TransactionalMapConfig extends PrimitiveConfig {
  @Override
  public PrimitiveType getType() {
    return AtomicMapType.instance();
  }
}
