// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
   * @param participants the set of participants in the transaction
   * @return a future to be completed once the transaction state has been updated
   */
  CompletableFuture<Void> preparing(TransactionId transactionId, Set<ParticipantInfo> participants);

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
