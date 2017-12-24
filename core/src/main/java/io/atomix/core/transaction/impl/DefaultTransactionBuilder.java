/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.transaction.impl;

import io.atomix.core.transaction.Transaction;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionService;
import io.atomix.primitive.PrimitiveManagementService;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default transaction builder.
 */
public class DefaultTransactionBuilder extends TransactionBuilder {
  private final PrimitiveManagementService managementService;
  private final TransactionService transactionService;

  public DefaultTransactionBuilder(
      String name,
      PrimitiveManagementService managementService,
      TransactionService transactionService) {
    super(name);
    this.managementService = checkNotNull(managementService);
    this.transactionService = checkNotNull(transactionService);
  }

  @Override
  public CompletableFuture<Transaction> buildAsync() {
    return CompletableFuture.completedFuture(new DefaultTransaction(transactionService, managementService, isolation()).sync());
  }
}
