/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.semaphore.impl;

import io.atomix.core.semaphore.AsyncDistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.semaphore.QueueStatus;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingDistributedSemaphore extends Synchronous<AsyncDistributedSemaphore> implements DistributedSemaphore {

  private final AsyncDistributedSemaphore asyncSemaphore;
  private final Duration timeout;

  public BlockingDistributedSemaphore(AsyncDistributedSemaphore asyncDistributedSemaphore, Duration timeout) {
    super(asyncDistributedSemaphore);
    this.asyncSemaphore = asyncDistributedSemaphore;
    this.timeout = timeout;
  }

  @Override
  public void acquire() {
    complete(asyncSemaphore.acquire(), 1);
  }

  @Override
  public void acquire(int permits) {
    complete(asyncSemaphore.acquire(permits), permits);
  }

  @Override
  public boolean tryAcquire() {
    return complete(asyncSemaphore.tryAcquire(), 1).isPresent();
  }

  @Override
  public boolean tryAcquire(int permits) {
    return complete(asyncSemaphore.tryAcquire(permits), permits).isPresent();
  }

  @Override
  public boolean tryAcquire(Duration timeout) {
    return complete(asyncSemaphore.tryAcquire(timeout), 1).isPresent();
  }

  @Override
  public boolean tryAcquire(int permits, Duration timeout) {
    return complete(asyncSemaphore.tryAcquire(permits, timeout), permits).isPresent();
  }

  @Override
  public void release() {
    complete(asyncSemaphore.release());
  }

  @Override
  public void release(int permits) {
    complete(asyncSemaphore.release(permits));
  }

  @Override
  public int availablePermits() {
    return complete(asyncSemaphore.availablePermits()).value();
  }

  @Override
  public int drainPermits() {
    return complete(asyncSemaphore.drainPermits()).value();
  }

  @Override
  public int increase(int permits) {
    return complete(asyncSemaphore.increase(permits)).value();
  }

  @Override
  public int reduce(int permits) {
    return complete(asyncSemaphore.reduce(permits)).value();
  }

  @Override
  public QueueStatus queueStatus() {
    return complete(asyncSemaphore.queueStatus()).value();
  }

  @Override
  public AsyncDistributedSemaphore async() {
    return asyncSemaphore;
  }

  private <T> T complete(CompletableFuture<T> future) {
    return complete(future, 0);
  }

  /**
   * Use for complete acquire or tryAcquire.
   * If interrupt or timeout before the future completed, set needRelease to true.
   * When the future completes, release these permits.
   */
  private <T> T complete(CompletableFuture<T> future, int acquirePermits) {
    AtomicBoolean needRelease = new AtomicBoolean(false);
    try {
      return future.whenComplete((version, error) -> {
        if (needRelease.get() && version != null) {
          if (acquirePermits > 0) {
            asyncSemaphore.release(acquirePermits);
          }
        }
      }).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      needRelease.set(acquirePermits > 0);
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      needRelease.set(acquirePermits > 0);
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      needRelease.set(acquirePermits > 0);
      throw new PrimitiveException(e.getCause());
    }
  }
}
