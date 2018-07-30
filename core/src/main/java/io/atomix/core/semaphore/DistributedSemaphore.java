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
