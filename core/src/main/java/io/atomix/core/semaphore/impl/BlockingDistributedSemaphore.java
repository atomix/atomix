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
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingDistributedSemaphore extends DistributedSemaphore {

  private final AsyncDistributedSemaphore asyncSemaphore;
  private final Duration timeout;

  public BlockingDistributedSemaphore(AsyncDistributedSemaphore asyncSemaphore, Duration timeout) {
    this.asyncSemaphore = asyncSemaphore;
    this.timeout = timeout;
  }

  @Override
  public String name() {
    return asyncSemaphore.name();
  }

  @Override
  public PrimitiveType type() {
    return asyncSemaphore.type();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return asyncSemaphore.protocol();
  }

  @Override
  public void acquireUninterruptibly() {
    complete(asyncSemaphore.acquire());
  }

  @Override
  public void acquire(int permits) throws InterruptedException {
    complete(asyncSemaphore.acquire(permits));
  }

  @Override
  public void acquireUninterruptibly(int permits) {
    complete(asyncSemaphore.acquire(permits));
  }

  @Override
  public boolean tryAcquire(int permits) {
    return complete(asyncSemaphore.tryAcquire(permits));
  }

  @Override
  public boolean tryAcquire(int permits, Duration timeout) throws InterruptedException {
    return complete(asyncSemaphore.tryAcquire(permits, timeout));
  }

  @Override
  public boolean tryAcquire() {
    return complete(asyncSemaphore.tryAcquire());
  }

  @Override
  public boolean tryAcquire(Duration timeout) throws InterruptedException {
    return complete(asyncSemaphore.tryAcquire(timeout));
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
    return complete(asyncSemaphore.availablePermits());
  }

  @Override
  public int drainPermits() {
    return complete(asyncSemaphore.drainPermits());
  }

  @Override
  protected void reducePermits(int reduction) {
    complete(asyncSemaphore.reducePermits(reduction));
  }

  @Override
  public boolean isFair() {
    return true;
  }

  @Override
  public AsyncDistributedSemaphore async() {
    return asyncSemaphore;
  }

  @Override
  public void close() {
    complete(asyncSemaphore.close());
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
      return future.thenApply(version -> {
        if (needRelease.get() && version != null) {
          if (acquirePermits > 0) {
            asyncSemaphore.release(acquirePermits);
          }
        }
        return version;
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
