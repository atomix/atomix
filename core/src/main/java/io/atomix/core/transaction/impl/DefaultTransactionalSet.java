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
package io.atomix.core.transaction.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import io.atomix.core.transaction.AsyncTransactionalMap;
import io.atomix.core.transaction.AsyncTransactionalSet;
import io.atomix.core.transaction.TransactionalSet;

/**
 * Default transactional set.
 */
public class DefaultTransactionalSet<E> implements AsyncTransactionalSet<E> {
  private final AsyncTransactionalMap<E, Boolean> transactionalMap;

  public DefaultTransactionalSet(AsyncTransactionalMap<E, Boolean> transactionalMap) {
    this.transactionalMap = transactionalMap;
  }

  @Override
  public String name() {
    return transactionalMap.name();
  }

  @Override
  public CompletableFuture<Boolean> add(E element) {
    return transactionalMap.put(element, true);
  }

  @Override
  public CompletableFuture<Boolean> remove(E element) {
    return transactionalMap.remove(element);
  }

  @Override
  public CompletableFuture<Boolean> contains(E element) {
    return transactionalMap.containsKey(element);
  }

  @Override
  public CompletableFuture<Void> close() {
    return transactionalMap.close();
  }

  @Override
  public TransactionalSet<E> sync(Duration operationTimeout) {
    return new BlockingTransactionalSet<E>(this, operationTimeout.toMillis());
  }
}
