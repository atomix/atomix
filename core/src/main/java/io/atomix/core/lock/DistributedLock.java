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
