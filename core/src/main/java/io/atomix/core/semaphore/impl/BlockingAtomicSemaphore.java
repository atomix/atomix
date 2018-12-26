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

import com.google.common.base.Throwables;
import io.atomix.core.semaphore.AsyncAtomicSemaphore;
import io.atomix.core.semaphore.AtomicSemaphore;
import io.atomix.core.semaphore.QueueStatus;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;
import io.atomix.utils.time.Version;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingAtomicSemaphore extends Synchronous<AsyncAtomicSemaphore> implements AtomicSemaphore {

  private final AsyncAtomicSemaphore asyncSemaphore;
  private final Duration timeout;

  public BlockingAtomicSemaphore(AsyncAtomicSemaphore asyncAtomicSemaphore, Duration timeout) {
    super(asyncAtomicSemaphore);
    this.asyncSemaphore = asyncAtomicSemaphore;
    this.timeout = timeout;
  }

  @Override
  public Version acquire() {
    return complete(asyncSemaphore.acquire(), 1);
  }

  @Override
  public Version acquire(int permits) {
    return complete(asyncSemaphore.acquire(permits), permits);
  }

  @Override
  public Optional<Version> tryAcquire() {
    return complete(asyncSemaphore.tryAcquire(), 1);
  }

  @Override
  public Optional<Version> tryAcquire(int permits) {
    return complete(asyncSemaphore.tryAcquire(permits), permits);
  }

  @Override
  public Optional<Version> tryAcquire(Duration timeout) {
    return complete(asyncSemaphore.tryAcquire(timeout), 1);
  }

  @Override
  public Optional<Version> tryAcquire(int permits, Duration timeout) {
    return complete(asyncSemaphore.tryAcquire(permits, timeout), permits);
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
  public int increasePermits(int permits) {
    return complete(asyncSemaphore.increasePermits(permits));
  }

  @Override
  public int reducePermits(int permits) {
    return complete(asyncSemaphore.reducePermits(permits));
  }

  @Override
  public QueueStatus queueStatus() {
    return complete(asyncSemaphore.queueStatus());
  }

  @Override
  public AsyncAtomicSemaphore async() {
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
      Throwable cause = Throwables.getRootCause(e);
      if (cause instanceof PrimitiveException) {
        throw (PrimitiveException) cause;
      } else {
        throw new PrimitiveException(cause);
      }
    }
  }
}
