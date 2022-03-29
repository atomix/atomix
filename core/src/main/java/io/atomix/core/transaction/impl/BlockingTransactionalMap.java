// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
