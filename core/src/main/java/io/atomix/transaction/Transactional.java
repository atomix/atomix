/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.transaction;

import io.atomix.time.Version;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for transactional primitives.
 */
public interface Transactional<T> {

  /**
   * Begins the transaction.
   *
   * @param transactionId the transaction identifier for the transaction to begin
   * @return a completable future to be completed with the lock version
   */
  CompletableFuture<Version> begin(TransactionId transactionId);

  /**
   * Prepares a transaction for commitment.
   *
   * @param transactionLog transaction log
   * @return {@code true} if prepare is successful and transaction is ready to be committed
   * {@code false} otherwise
   */
  CompletableFuture<Boolean> prepare(TransactionLog<T> transactionLog);

  /**
   * Prepares and commits a transaction.
   *
   * @param transactionLog transaction log
   * @return {@code true} if prepare is successful and transaction was committed
   * {@code false} otherwise
   */
  CompletableFuture<Boolean> prepareAndCommit(TransactionLog<T> transactionLog);

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