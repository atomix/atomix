// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set;

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.Transactional;
import io.atomix.primitive.DistributedPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * A distributed collection designed for holding unique elements.
 * <p>
 * All methods of {@code AsyncDistributedSet} immediately return a {@link CompletableFuture future}.
 * The returned future will be {@link CompletableFuture#complete completed} when the operation
 * completes.
 *
 * @param <E> set entry type
 */
public interface AsyncDistributedSet<E> extends AsyncDistributedCollection<E>, Transactional<SetUpdate<E>> {
  @Override
  default DistributedSet<E> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedSet<E> sync(Duration operationTimeout);
}
