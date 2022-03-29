// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.protocol.ProxyProtocol;

import java.util.concurrent.CompletableFuture;

/**
 * Transaction participant.
 */
public interface TransactionParticipant<T> extends DistributedPrimitive {
  @Override
  ProxyProtocol protocol();

  /**
   * Returns the participant's transaction log.
   *
   * @return the participant's transaction log
   */
  TransactionLog<T> log();

  /**
   * Prepares the participant.
   *
   * @return a future to be completed with a boolean indicating whether the participant's transaction was successfully prepared
   */
  CompletableFuture<Boolean> prepare();

  /**
   * Commits the participant.
   *
   * @return a future to be completed once the participant has been committed
   */
  CompletableFuture<Void> commit();

  /**
   * Rolls back the participant.
   *
   * @return a future to be completed once the participant has been rolled back
   */
  CompletableFuture<Void> rollback();

  /**
   * Closes the participant.
   *
   * @return a future to be completed once the participant has been closed
   */
  CompletableFuture<Void> close();

}
