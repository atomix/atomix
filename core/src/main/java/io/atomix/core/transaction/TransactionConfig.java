// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveType;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transaction configuration.
 */
public class TransactionConfig extends PrimitiveConfig<TransactionConfig> {
  private Isolation isolation = Isolation.READ_COMMITTED;

  @Override
  public PrimitiveType getType() {
    return TransactionType.instance();
  }

  /**
   * Sets the transaction isolation level.
   *
   * @param isolation the transaction isolation level
   * @return the transaction configuration
   */
  public TransactionConfig setIsolation(Isolation isolation) {
    this.isolation = checkNotNull(isolation, "isolation cannot be null");
    return this;
  }

  /**
   * Returns the transaction isolation level.
   *
   * @return the transaction isolation level
   */
  public Isolation getIsolation() {
    return isolation;
  }
}
