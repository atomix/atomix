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

import io.atomix.core.transaction.AsyncTransactionalMap;
import io.atomix.core.transaction.TransactionalMap;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Delegating transactional map.
 */
public class DelegatingTransactionalMap<K, V> implements AsyncTransactionalMap<K, V> {
  private final AsyncTransactionalMap<K, V> map;

  public DelegatingTransactionalMap(AsyncTransactionalMap<K, V> map) {
    this.map = map;
  }

  @Override
  public String name() {
    return map.name();
  }

  @Override
  public PrimitiveType type() {
    return map.type();
  }

  @Override
  public ProxyProtocol protocol() {
    return (ProxyProtocol) map.protocol();
  }

  @Override
  public CompletableFuture<V> get(K key) {
    return map.get(key);
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return map.containsKey(key);
  }

  @Override
  public CompletableFuture<V> put(K key, V value) {
    return map.put(key, value);
  }

  @Override
  public CompletableFuture<V> remove(K key) {
    return map.remove(key);
  }

  @Override
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return map.putIfAbsent(key, value);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return map.remove(key, value);
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return map.replace(key, oldValue, newValue);
  }

  @Override
  public CompletableFuture<Void> close() {
    return map.close();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return map.delete();
  }

  @Override
  public TransactionalMap<K, V> sync(Duration operationTimeout) {
    return new BlockingTransactionalMap<>(this, operationTimeout.toMillis());
  }
}
