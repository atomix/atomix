// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.idgenerator;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * An async ID generator for generating globally unique numbers.
 */
public interface AsyncAtomicIdGenerator extends AsyncPrimitive {

  /**
   * Returns the next globally unique numeric ID.
   *
   * @return a future to be completed with the next globally unique identifier
   */
  CompletableFuture<Long> nextId();

  @Override
  default AtomicIdGenerator sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  AtomicIdGenerator sync(Duration operationTimeout);
}
