/*
 * Copyright 2015-present Open Networking Foundation
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

import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.map.TransactionalMap;
import io.atomix.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Provides a context for transactional operations.
 * <p>
 * A transaction context is a vehicle for grouping operations into a unit with the
 * properties of atomicity, isolation, and durability. Transactions also provide the
 * ability to maintain an application's invariants or integrity constraints,
 * supporting the property of consistency. Together these properties are known as ACID.
 * <p>
 * A transaction context provides a boundary within which transactions
 * are run. It also is a place where all modifications made within a transaction
 * are cached until the point when the transaction commits or aborts. It thus ensures
 * isolation of work happening with in the transaction boundary. Within a transaction
 * context isolation level is REPEATABLE_READS i.e. only data that is committed can be read.
 * The only uncommitted data that can be read is the data modified by the current transaction.
 */
public interface TransactionContext extends DistributedPrimitive {

  @Override
  default DistributedPrimitive.Type primitiveType() {
    return DistributedPrimitive.Type.TRANSACTION_CONTEXT;
  }

  /**
   * Returns the transaction identifier.
   *
   * @return transaction id
   */
  TransactionId transactionId();

  /**
   * Returns if this transaction context is open.
   *
   * @return true if open, false otherwise
   */
  boolean isOpen();

  /**
   * Starts a new transaction.
   */
  void begin();

  /**
   * Commits a transaction that was previously started thereby making its changes permanent
   * and externally visible.
   *
   * @return A future that will be completed when the operation completes
   */
  CompletableFuture<CommitStatus> commit();

  /**
   * Aborts any changes made in this transaction context and discarding all locally cached updates.
   */
  void abort();

  /**
   * Returns a transactional map data structure with the specified name.
   *
   * @param <K>        key type
   * @param <V>        value type
   * @param mapName    name of the transactional map
   * @param serializer serializer to use for encoding/decoding keys and values of the map
   * @return Transactional Map
   */
  <K, V> TransactionalMap<K, V> getTransactionalMap(String mapName, Serializer serializer);
}
