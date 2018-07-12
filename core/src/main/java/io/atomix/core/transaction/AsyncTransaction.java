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

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.protocol.ProxyProtocol;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous transaction.
 */
public interface AsyncTransaction extends AsyncPrimitive {

  /**
   * Returns the transaction identifier.
   *
   * @return transaction id
   */
  TransactionId transactionId();

  /**
   * Returns the transaction isolation level.
   *
   * @return the transaction isolation level
   */
  Isolation isolation();

  /**
   * Returns if this transaction context is open.
   *
   * @return true if open, false otherwise
   */
  boolean isOpen();

  /**
   * Starts a new transaction.
   *
   * @return a future to be completed once the transaction has been started
   */
  CompletableFuture<Void> begin();

  /**
   * Commits a transaction that was previously started thereby making its changes permanent
   * and externally visible.
   *
   * @return A future that will be completed when the operation completes
   */
  CompletableFuture<CommitStatus> commit();

  /**
   * Aborts any changes made in this transaction context and discarding all locally cached updates.
   *
   * @return a future to be completed once the transaction has been aborted
   */
  CompletableFuture<Void> abort();

  /**
   * Returns a new transactional map builder.
   *
   * @param name the map name
   * @param <K>  the key type
   * @param <V>  the value type
   * @return the transactional map builder
   */
  <K, V> TransactionalMapBuilder<K, V> mapBuilder(String name);

  /**
   * Returns a new transactional map builder.
   *
   * @param name the map name
   * @param protocol the map protocol
   * @param <K>  the key type
   * @param <V>  the value type
   * @return the transactional map builder
   */
  default <K, V> TransactionalMapBuilder<K, V> mapBuilder(String name, ProxyProtocol protocol) {
    return this.<K, V>mapBuilder(name).withProtocol(protocol);
  }

  /**
   * Returns a new transactional set builder.
   *
   * @param name the set name
   * @param <E>  the set element type
   * @return the transactional set builder
   */
  <E> TransactionalSetBuilder<E> setBuilder(String name);

  /**
   * Returns a new transactional set builder.
   *
   * @param name the set name
   * @param protocol the map protocol
   * @param <E>  the set element type
   * @return the transactional set builder
   */
  default <E> TransactionalSetBuilder<E> setBuilder(String name, ProxyProtocol protocol) {
    return this.<E>setBuilder(name).withProtocol(protocol);
  }

  @Override
  default Transaction sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  Transaction sync(Duration operationTimeout);
}
