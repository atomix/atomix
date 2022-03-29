// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.semaphore;

import io.atomix.primitive.SyncPrimitive;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Distributed semaphore.
 */
public abstract class DistributedSemaphore extends Semaphore implements SyncPrimitive {
  public DistributedSemaphore() {
    super(0);
  }

  @Override
  public abstract void acquire() throws InterruptedException;

  @Override
  public abstract void acquireUninterruptibly();

  @Override
  public abstract boolean tryAcquire();

  @Override
  public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
    return tryAcquire(Duration.ofMillis(unit.toMillis(timeout)));
  }

  public abstract boolean tryAcquire(Duration timeout) throws InterruptedException;

  @Override
  public abstract void release();

  @Override
  public abstract void acquire(int permits) throws InterruptedException;

  @Override
  public abstract void acquireUninterruptibly(int permits);

  @Override
  public abstract boolean tryAcquire(int permits);

  @Override
  public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
    return tryAcquire(permits, Duration.ofMillis(unit.toMillis(timeout)));
  }

  public abstract boolean tryAcquire(int permits, Duration timeout) throws InterruptedException;

  @Override
  public abstract void release(int permits);

  @Override
  public abstract int availablePermits();

  @Override
  public abstract int drainPermits();

  protected abstract void increasePermits(int increase);

  @Override
  protected abstract void reducePermits(int reduction);

  @Override
  public abstract boolean isFair();

  @Override
  public abstract AsyncDistributedSemaphore async();
}
