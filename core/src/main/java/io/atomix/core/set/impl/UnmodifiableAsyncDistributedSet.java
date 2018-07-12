/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.set.impl;

import io.atomix.core.collection.impl.UnmodifiableAsyncDistributedCollection;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Unmodifiable distributed set.
 */
public class UnmodifiableAsyncDistributedSet<E> extends UnmodifiableAsyncDistributedCollection<E> implements AsyncDistributedSet<E> {
  private static final String ERROR_MSG = "updates are not allowed";

  public UnmodifiableAsyncDistributedSet(AsyncDistributedSet<E> delegateCollection) {
    super(delegateCollection);
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<E>> transactionLog) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Override
  public DistributedSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}
