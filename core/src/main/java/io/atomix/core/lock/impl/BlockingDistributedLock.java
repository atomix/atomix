// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock.impl;

import com.google.common.base.Throwables;
import io.atomix.core.lock.AsyncDistributedLock;
import io.atomix.core.lock.DistributedLock;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;

/**
 * Default implementation for a {@code DistributedLock} backed by a {@link AsyncDistributedLock}.
 */
public class BlockingDistributedLock extends Synchronous<AsyncDistributedLock> implements DistributedLock {

  private final AsyncDistributedLock asyncLock;
  private final long operationTimeoutMillis;

  public BlockingDistributedLock(AsyncDistributedLock asyncLock, long operationTimeoutMillis) {
    super(asyncLock);
    this.asyncLock = asyncLock;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public void lock() {
    asyncLock.lock().join();
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    asyncLock.lock().join();
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean tryLock() {
    return asyncLock.tryLock().join();
  }

  @Override
  public boolean tryLock(Duration timeout) {
    return asyncLock.tryLock(timeout).join();
  }

  @Override
  public void unlock() {
    complete(asyncLock.unlock());
  }

  @Override
  public boolean isLocked() {
    return complete(asyncLock.isLocked());
  }

  @Override
  public AsyncDistributedLock async() {
    return asyncLock;
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
