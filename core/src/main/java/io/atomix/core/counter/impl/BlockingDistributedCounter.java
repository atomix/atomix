// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter.impl;

import io.atomix.core.counter.AsyncDistributedCounter;
import io.atomix.core.counter.DistributedCounter;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation for a {@code DistributedCounter} backed by a {@link AsyncDistributedCounter}.
 */
public class BlockingDistributedCounter extends Synchronous<AsyncDistributedCounter> implements DistributedCounter {

  private final AsyncDistributedCounter asyncCounter;
  private final long operationTimeoutMillis;

  public BlockingDistributedCounter(AsyncDistributedCounter asyncCounter, long operationTimeoutMillis) {
    super(asyncCounter);
    this.asyncCounter = asyncCounter;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public long incrementAndGet() {
    return complete(asyncCounter.incrementAndGet());
  }

  @Override
  public long decrementAndGet() {
    return complete(asyncCounter.decrementAndGet());
  }

  @Override
  public long getAndIncrement() {
    return complete(asyncCounter.getAndIncrement());
  }

  @Override
  public long getAndDecrement() {
    return complete(asyncCounter.getAndDecrement());
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
  public long get() {
    return complete(asyncCounter.get());
  }

  @Override
  public AsyncDistributedCounter async() {
    return asyncCounter;
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
