// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.log;

import java.util.concurrent.CompletableFuture;

/**
 * Log producer.
 */
public interface LogProducer {

  /**
   * Appends the given entry to the log.
   *
   * @param value the entry to append
   * @return a future to be completed once the entry has been appended
   */
  CompletableFuture<Long> append(byte[] value);

}
