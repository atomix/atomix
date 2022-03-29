// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;

/**
 * Transaction builder.
 */
public abstract class TransactionBuilder extends PrimitiveBuilder<TransactionBuilder, TransactionConfig, Transaction> {
  protected TransactionBuilder(String name, TransactionConfig config, PrimitiveManagementService managementService) {
    super(TransactionType.instance(), name, config, managementService);
  }

  /**
   * Sets the transaction isolation level.
   *
   * @param isolation the transaction isolation level
   * @return the transaction builder
   */
  public TransactionBuilder withIsolation(Isolation isolation) {
    config.setIsolation(isolation);
    return this;
  }
}
