// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.primitive.SyncPrimitive;

/**
 * Transaction primitive.
 */
public interface Transaction extends SyncPrimitive {

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
   */
  void begin();

  /**
   * Commits a transaction that was previously started thereby making its changes permanent
   * and externally visible.
   *
   * @return indicates whether the transaction was successful
   */
  CommitStatus commit();

  /**
   * Aborts any changes made in this transaction context and discarding all locally cached updates.
   */
  void abort();

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
   * Returns a new transactional set builder.
   *
   * @param name the set name
   * @param <E>  the set element type
   * @return the transactional set builder
   */
  <E> TransactionalSetBuilder<E> setBuilder(String name);

  @Override
  AsyncTransaction async();
}
