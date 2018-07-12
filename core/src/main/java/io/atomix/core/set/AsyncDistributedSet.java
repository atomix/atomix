/*
 * Copyright 2016-present Open Networking Foundation
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
