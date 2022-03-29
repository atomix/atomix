// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import io.atomix.core.collection.impl.DistributedCollectionService;
import io.atomix.core.transaction.impl.CommitResult;
import io.atomix.core.transaction.impl.PrepareResult;
import io.atomix.core.transaction.impl.RollbackResult;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.operation.Command;

/**
 * Distributed set service.
 */
public interface DistributedSetService<E> extends DistributedCollectionService<E> {

  /**
   * Prepares and commits a transaction.
   *
   * @param transactionLog the transaction log
   * @return the prepare result
   */
  @Command
  PrepareResult prepareAndCommit(TransactionLog<SetUpdate<E>> transactionLog);

  /**
   * Prepares a transaction.
   *
   * @param transactionLog the transaction log
   * @return the prepare result
   */
  @Command
  PrepareResult prepare(TransactionLog<SetUpdate<E>> transactionLog);

  /**
   * Commits a transaction.
   *
   * @param transactionId the transaction identifier
   * @return the commit result
   */
  @Command
  CommitResult commit(TransactionId transactionId);

  /**
   * Rolls back a transaction.
   *
   * @param transactionId the transaction identifier
   * @return the rollback result
   */
  @Command
  RollbackResult rollback(TransactionId transactionId);

}
