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

import com.google.common.base.Throwables;
import io.atomix.core.transaction.AsyncTransactionalMap;
import io.atomix.core.transaction.TransactionalMap;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Blocking transactional map.
 */
public class BlockingTransactionalMap<K, V> implements TransactionalMap<K, V> {
  private final AsyncTransactionalMap<K, V> asyncMap;
  private final long operationTimeoutMillis;

  public BlockingTransactionalMap(AsyncTransactionalMap<K, V> asyncMap, long operationTimeoutMillis) {
    this.asyncMap = asyncMap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public String name() {
    return asyncMap.name();
  }

  @Override
  public PrimitiveType type() {
    return asyncMap.type();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return asyncMap.protocol();
  }

  @Override
  public V get(K key) {
    return complete(asyncMap.get(key));
  }

  @Override
  public boolean containsKey(K key) {
    return complete(asyncMap.containsKey(key));
  }

  @Override
  public V put(K key, V value) {
    return complete(asyncMap.put(key, value));
  }

  @Override
  public V remove(K key) {
    return complete(asyncMap.remove(key));
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return complete(asyncMap.putIfAbsent(key, value));
  }

  @Override
  public boolean remove(K key, V value) {
    return complete(asyncMap.remove(key, value));
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return complete(asyncMap.replace(key, oldValue, newValue));
  }

  @Override
  public void close() {
    complete(asyncMap.close());
  }

  @Override
  public AsyncTransactionalMap<K, V> async() {
    return asyncMap;
  }

  private <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause());
      throw new PrimitiveException(e.getCause());
    }
  }
}
