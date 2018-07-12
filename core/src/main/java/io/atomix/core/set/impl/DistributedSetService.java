/*
 * Copyright 2018-present Open Networking Foundation
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
