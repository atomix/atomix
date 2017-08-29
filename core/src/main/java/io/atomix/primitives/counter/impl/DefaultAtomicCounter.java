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
package io.atomix.primitives.counter.impl;

import io.atomix.primitives.PrimitiveException;
import io.atomix.primitives.Synchronous;
import io.atomix.primitives.counter.AsyncAtomicCounter;
import io.atomix.primitives.counter.AtomicCounter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation for a {@code AtomicCounter} backed by a {@link AsyncAtomicCounter}.
 */
public class DefaultAtomicCounter extends Synchronous<AsyncAtomicCounter> implements AtomicCounter {

  private final AsyncAtomicCounter asyncCounter;
  private final long operationTimeoutMillis;

  public DefaultAtomicCounter(AsyncAtomicCounter asyncCounter, long operationTimeoutMillis) {
    super(asyncCounter);
    this.asyncCounter = asyncCounter;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public long incrementAndGet() {
    return complete(asyncCounter.incrementAndGet());
  }

  @Override
  public long getAndIncrement() {
    return complete(asyncCounter.getAndIncrement());
  }

  @Override
  public long getAndAdd(long delta) {
    return complete(asyncCounter.getAndAdd(delta));
  }

  @Override
  public long addAndGet(long delta) {
    return complete(asyncCounter.addAndGet(delta));
  }

  @Override
  public void set(long value) {
    complete(asyncCounter.set(value));
  }

  @Override
  public boolean compareAndSet(long expectedValue, long updateValue) {
    return complete(asyncCounter.compareAndSet(expectedValue, updateValue));
  }

  @Override
  public long get() {
    return complete(asyncCounter.get());
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
      throw new PrimitiveException(e.getCause());
    }
  }
}
