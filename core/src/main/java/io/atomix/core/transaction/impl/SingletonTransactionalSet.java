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

import java.util.concurrent.CompletableFuture;

import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.transaction.TransactionParticipant;

/**
 * Singleton transactional set.
 */
public class SingletonTransactionalSet<E> extends DelegatingTransactionalSet<E> implements TransactionParticipant<SetUpdate<E>> {
  private final TransactionalSetParticipant<E> set;

  public SingletonTransactionalSet(TransactionalSetParticipant<E> set) {
    super(set);
    this.set = set;
  }

  @Override
  public TransactionLog<SetUpdate<E>> log() {
    return set.log();
  }

  @Override
  public CompletableFuture<Boolean> prepare() {
    return set.prepare();
  }

  @Override
  public CompletableFuture<Void> commit() {
    return set.commit();
  }

  @Override
  public CompletableFuture<Void> rollback() {
    return set.rollback();
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return CompletableFuture.completedFuture(null);
  }
}
