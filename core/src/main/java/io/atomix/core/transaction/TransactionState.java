// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

/**
 * Transaction state.
 * <p>
 * The transaction state is used to indicate the phase within which the transaction is currently running.
 */
public enum TransactionState {

  /**
   * Active transaction state.
   * <p>
   * The {@code ACTIVE} state represents a transaction in progress. Active transactions may or may not affect
   * concurrently running transactions depending on the transaction's isolation level.
   */
  ACTIVE,

  /**
   * Preparing transaction state.
   * <p>
   * Once a transaction commitment begins, it enters the {@code PREPARING} phase of the two-phase commit protocol.
   */
  PREPARING,

  /**
   * Prepared transaction state.
   * <p>
   * Once the first phase of the two-phase commit protocol is complete, the transaction's state is set to
   * {@code PREPARED}.
   */
  PREPARED,

  /**
   * Committing transaction state.
   * <p>
   * The {@code COMMITTING} state represents a transaction within the second phase of the two-phase commit
   * protocol.
   */
  COMMITTING,

  /**
   * Committed transaction state.
   * <p>
   * Once the second phase of the two-phase commit protocol is complete, the transaction's state is set to
   * {@code COMMITTED}.
   */
  COMMITTED,

  /**
   * Rolling back transaction state.
   * <p>
   * In the event of a two-phase lock failure, when the transaction is rolled back it will enter the
   * {@code ROLLING_BACK} state while the rollback is in progress.
   */
  ROLLING_BACK,

  /**
   * Rolled back transaction state.
   * <p>
   * Once a transaction has been rolled back, it will enter the {@code ROLLED_BACK} state.
   */
  ROLLED_BACK,
}
