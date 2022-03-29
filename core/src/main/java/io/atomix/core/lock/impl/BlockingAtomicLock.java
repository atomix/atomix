// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock.impl;

import com.google.common.base.Throwables;
import io.atomix.core.lock.AsyncAtomicLock;
import io.atomix.core.lock.AtomicLock;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;
import io.atomix.utils.time.Version;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation for a {@code DistributedLock} backed by a {@link AsyncAtomicLock}.
 */
public class BlockingAtomicLock extends Synchronous<AsyncAtomicLock> implements AtomicLock {

  private final AsyncAtomicLock asyncLock;
  private final long operationTimeoutMillis;

  public BlockingAtomicLock(AsyncAtomicLock asyncLock, long operationTimeoutMillis) {
    super(asyncLock);
    this.asyncLock = asyncLock;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public Version lock() {
    return asyncLock.lock().join();
  }

  @Override
  public Optional<Version> tryLock() {
    return asyncLock.tryLock().join();
  }

  @Override
  public Optional<Version> tryLock(Duration timeout) {
    return asyncLock.tryLock(timeout).join();
  }

  @Override
  public boolean isLocked() {
    return complete(asyncLock.isLocked());
  }

  @Override
  public boolean isLocked(Version version) {
    return complete(asyncLock.isLocked(version));
  }

  @Override
  public void unlock() {
    complete(asyncLock.unlock());
  }

  @Override
  public AsyncAtomicLock async() {
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
