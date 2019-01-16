/*
 * Copyright 2019-present Open Networking Foundation
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

import io.atomix.core.transaction.AsyncTransactionalSet;
import io.atomix.core.transaction.TransactionalSet;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Delegating transactional set.
 */
public abstract class DelegatingTransactionalSet<E> implements AsyncTransactionalSet<E> {
  private final AsyncTransactionalSet<E> set;

  public DelegatingTransactionalSet(AsyncTransactionalSet<E> set) {
    this.set = set;
  }

  @Override
  public String name() {
    return set.name();
  }

  @Override
  public PrimitiveType type() {
    return set.type();
  }

  @Override
  public ProxyProtocol protocol() {
    return (ProxyProtocol) set.protocol();
  }

  @Override
  public CompletableFuture<Boolean> add(E e) {
    return set.add(e);
  }

  @Override
  public CompletableFuture<Boolean> remove(E e) {
    return set.remove(e);
  }

  @Override
  public CompletableFuture<Boolean> contains(E e) {
    return set.contains(e);
  }

  @Override
  public CompletableFuture<Void> close() {
    return set.close();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return set.delete();
  }

  @Override
  public TransactionalSet<E> sync(Duration operationTimeout) {
    return new BlockingTransactionalSet<>(set, operationTimeout.toMillis());
  }
}
