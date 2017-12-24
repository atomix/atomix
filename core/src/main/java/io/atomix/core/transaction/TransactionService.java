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
package io.atomix.core.transaction;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Transaction service.
 */
public interface TransactionService {

  /**
   * Returns the set of active transactions.
   *
   * @return the set of active transactions
   */
  Set<TransactionId> getActiveTransactions();

  /**
   * Returns a transaction state by ID.
   *
   * @param transactionId the transaction ID
   * @return the transaction state
   */
  TransactionState getTransactionState(TransactionId transactionId);

  /**
   * Returns a new transaction ID.
   *
   * @return the new transaction ID
   */
  CompletableFuture<TransactionId> begin();

  /**
   * Marks the given transaction as preparing.
   *
   * @param transactionId the transaction to mark as preparing
   * @return a future to be completed once the transaction state has been updated
   */
  CompletableFuture<Void> preparing(TransactionId transactionId);

  /**
   * Marks the given transaction as committing.
   *
   * @param transactionId the transaction to mark as committing
   * @return a future to be completed once the transaction state has been updated
   */
  CompletableFuture<Void> committing(TransactionId transactionId);

  /**
   * Marks the given transaction as rolling back.
   *
   * @param transactionId the transaction to mark as rolling back
   * @return a future to be completed once the transaction state has been updated
   */
  CompletableFuture<Void> aborting(TransactionId transactionId);

  /**
   * Marks the given transaction as complete.
   *
   * @param transactionId the transaction to mark as complete
   * @return a future to be completed once the transaction state has been cleared
   */
  CompletableFuture<Void> complete(TransactionId transactionId);

}
