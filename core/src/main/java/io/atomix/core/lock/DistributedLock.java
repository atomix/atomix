// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock;

import io.atomix.primitive.SyncPrimitive;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Distributed lock primitive.
 */
public interface DistributedLock extends SyncPrimitive, Lock {

  @Override
  default boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    return tryLock(Duration.ofMillis(unit.toMillis(time)));
  }

  /**
   * Acquires the lock if it is free within the given waiting time and the
   * current thread has not been {@linkplain Thread#interrupt interrupted}.
   *
   * @param timeout the timeout to wait to acquire the lock
   *
   * @throws InterruptedException if the current thread is interrupted
   *         while acquiring the lock (and interruption of lock
   *         acquisition is supported)
   */
  boolean tryLock(Duration timeout) throws InterruptedException;

  /**
   * Query whether this lock is locked or not.
   *
   * @return {@code true} if this lock is locked, {@code false} otherwise
   */
  boolean isLocked();

  @Override
  AsyncDistributedLock async();
}
