// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.log;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Log consumer.
 */
public interface LogConsumer {

  /**
   * Adds a new consumer.
   *
   * @param consumer the consumer to add
   * @return a future to be completed once the consumer has been added
   */
  default CompletableFuture<Void> consume(Consumer<LogRecord> consumer) {
    return consume(1, consumer);
  }

  /**
   * Adds a new consumer.
   *
   * @param index the index from which to begin consuming
   * @param consumer the consumer to add
   * @return a future to be completed once the consumer has been added
   */
  CompletableFuture<Void> consume(long index, Consumer<LogRecord> consumer);

}
