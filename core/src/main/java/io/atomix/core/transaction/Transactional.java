// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for transactional primitives.
 */
public interface Transactional<T> {

  /**
   * Prepares a transaction for commitment.
   *
   * @param transactionLog transaction log
   * @return {@code true} if prepare is successful and transaction is ready to be committed
   * {@code false} otherwise
   */
  CompletableFuture<Boolean> prepare(TransactionLog<T> transactionLog);

  /**
   * Commits a previously prepared transaction and unlocks the object.
   *
   * @param transactionId transaction identifier
   * @return future that will be completed when the operation finishes
   */
  CompletableFuture<Void> commit(TransactionId transactionId);

  /**
   * Aborts a previously prepared transaction and unlocks the object.
   *
   * @param transactionId transaction identifier
   * @return future that will be completed when the operation finishes
   */
  CompletableFuture<Void> rollback(TransactionId transactionId);

}
