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
