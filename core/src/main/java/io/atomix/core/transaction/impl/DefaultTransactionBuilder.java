// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction.impl;

import io.atomix.core.transaction.Transaction;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionConfig;
import io.atomix.core.transaction.TransactionService;
import io.atomix.primitive.PrimitiveManagementService;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default transaction builder.
 */
public class DefaultTransactionBuilder extends TransactionBuilder {
  private final TransactionService transactionService;

  public DefaultTransactionBuilder(
      String name,
      TransactionConfig config,
      PrimitiveManagementService managementService,
      TransactionService transactionService) {
    super(name, config, managementService);
    this.transactionService = checkNotNull(transactionService);
  }

  @Override
  public CompletableFuture<Transaction> buildAsync() {
    return CompletableFuture.completedFuture(new DefaultTransaction(transactionService, managementService, config.getIsolation()).sync());
  }
}
