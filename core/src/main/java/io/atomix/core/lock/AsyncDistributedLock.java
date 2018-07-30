/*
 * Copyright 2017-present Open Networking Foundation
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

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous lock primitive.
 */
public interface AsyncDistributedLock extends AsyncPrimitive {

  /**
   * Acquires the lock, blocking until it's available.
   *
   * @return future to be completed once the lock has been acquired
   */
  CompletableFuture<Void> lock();

  /**
   * Attempts to acquire the lock.
   *
   * @return future to be completed with a boolean indicating whether the lock was acquired
   */
  CompletableFuture<Boolean> tryLock();

  /**
   * Attempts to acquire the lock.
   *
   * @param timeout the timeout after which to give up attempting to acquire the lock
   * @param unit the timeout time unit
   * @return future to be completed with a boolean indicating whether the lock was acquired
   */
  default CompletableFuture<Boolean> tryLock(long timeout, TimeUnit unit) {
    return tryLock(Duration.ofMillis(unit.toMillis(timeout)));
  }

  /**
   * Attempts to acquire the lock for a specified amount of time.
   *
   * @param timeout the timeout after which to give up attempting to acquire the lock
   * @return future to be completed with a boolean indicating whether the lock was acquired
   */
  CompletableFuture<Boolean> tryLock(Duration timeout);

  /**
   * Unlocks the lock.
   *
   * @return future to be completed once the lock has been released
   */
  CompletableFuture<Void> unlock();

  /**
   * Query whether this lock is locked or not.
   *
   * @return future to be completed with a boolean indicating whether the lock was locked or not
   */
  CompletableFuture<Boolean> isLocked();

  @Override
  default DistributedLock sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedLock sync(Duration operationTimeout);
}
