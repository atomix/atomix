// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import com.google.common.base.Throwables;
import io.atomix.core.map.AsyncAtomicCounterMap;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation of {@code AtomicCounterMap}.
 *
 * @param <K> map key type
 */
public class BlockingAtomicCounterMap<K> extends Synchronous<AsyncAtomicCounterMap<K>> implements AtomicCounterMap<K> {

  private final AsyncAtomicCounterMap<K> asyncCounterMap;
  private final long operationTimeoutMillis;

  public BlockingAtomicCounterMap(AsyncAtomicCounterMap<K> asyncCounterMap, long operationTimeoutMillis) {
    super(asyncCounterMap);
    this.asyncCounterMap = asyncCounterMap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public long incrementAndGet(K key) {
    return complete(asyncCounterMap.incrementAndGet(key));
  }

  @Override
  public long decrementAndGet(K key) {
    return complete(asyncCounterMap.decrementAndGet(key));
  }

  @Override
  public long getAndIncrement(K key) {
    return complete(asyncCounterMap.getAndIncrement(key));
  }

  @Override
  public long getAndDecrement(K key) {
    return complete(asyncCounterMap.getAndDecrement(key));
  }

  @Override
  public long addAndGet(K key, long delta) {
    return complete(asyncCounterMap.addAndGet(key, delta));
  }

  @Override
  public long getAndAdd(K key, long delta) {
    return complete(asyncCounterMap.getAndAdd(key, delta));
  }

  @Override
  public long get(K key) {
    return complete(asyncCounterMap.get(key));
  }

  @Override
  public long put(K key, long newValue) {
    return complete(asyncCounterMap.put(key, newValue));
  }

  @Override
  public long putIfAbsent(K key, long newValue) {
    return complete(asyncCounterMap.putIfAbsent(key, newValue));
  }

  @Override
  public boolean replace(K key, long expectedOldValue, long newValue) {
    return complete(asyncCounterMap.replace(key, expectedOldValue, newValue));
  }

  @Override
  public long remove(K key) {
    return complete(asyncCounterMap.remove(key));
  }

  @Override
  public boolean remove(K key, long value) {
    return complete(asyncCounterMap.remove(key, value));
  }

  @Override
  public int size() {
    return complete(asyncCounterMap.size());
  }

  @Override
  public boolean isEmpty() {
    return complete(asyncCounterMap.isEmpty());
  }

  @Override
  public void clear() {
    complete(asyncCounterMap.clear());
  }

  @Override
  public AsyncAtomicCounterMap<K> async() {
    return asyncCounterMap;
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
      Throwable cause = Throwables.getRootCause(e);
      if (cause instanceof PrimitiveException) {
        throw (PrimitiveException) cause;
      } else {
        throw new PrimitiveException(cause);
      }
    }
  }
}
