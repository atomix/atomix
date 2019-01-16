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

import io.atomix.core.map.impl.MapUpdate;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.transaction.TransactionParticipant;

/**
 * Singleton transactional map.
 */
public class SingletonTransactionalMap<K, V> extends DelegatingTransactionalMap<K, V> implements TransactionParticipant<MapUpdate<K, V>> {
  private final TransactionalMapParticipant<K, V> map;

  public SingletonTransactionalMap(TransactionalMapParticipant<K, V> map) {
    super(map);
    this.map = map;
  }

  @Override
  public TransactionLog<MapUpdate<K, V>> log() {
    return map.log();
  }

  @Override
  public CompletableFuture<Boolean> prepare() {
    return map.prepare();
  }

  @Override
  public CompletableFuture<Void> commit() {
    return map.commit();
  }

  @Override
  public CompletableFuture<Void> rollback() {
    return map.rollback();
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
