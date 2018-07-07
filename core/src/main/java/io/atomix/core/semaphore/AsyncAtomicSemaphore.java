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

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.utils.time.Version;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;


/**
 * Distributed implementation of {@link java.util.concurrent.Semaphore}.
 */
public interface AsyncAtomicSemaphore extends AsyncPrimitive {

  /**
   * Acquires a permit from this semaphore.
   *
   * @return future to be completed once the permit has been acquired
   */
  CompletableFuture<Version> acquire();

  /**
   * Acquires the given number of permits from this semaphore.
   *
   * @return future to be completed once the permits has been acquired
   */
  CompletableFuture<Version> acquire(int permits);

  /**
   * Acquires a permit, if one is available and returns immediately.
   *
   * @return future to be completed with a boolean indicating whether the permit was acquired
   */
  CompletableFuture<Optional<Version>> tryAcquire();

  /**
   * Acquires the given number of permits, if they are available and returns immediately.
   *
   * @param permits permits to acquire
   * @return future to be completed with a boolean indicating whether the permits was acquired
   */
  CompletableFuture<Optional<Version>> tryAcquire(int permits);

  /**
   * Acquires a permit from this semaphore if one becomes available within the given waiting time.
   *
   * @param timeout the maximum time to wait for a permit
   * @return future to be completed with a boolean indicating whether the permit was acquired
   */
  CompletableFuture<Optional<Version>> tryAcquire(Duration timeout);

  /**
   * Acquires the given number of permits, if they are available within the given waiting time.
   *
   * @param permits permits to acquire
   * @param timeout the maximum time to wait for a permit
   * @return future to be completed with a boolean indicating whether the permits was acquired
   */
  CompletableFuture<Optional<Version>> tryAcquire(int permits, Duration timeout);

  /**
   * Releases a permit.
   *
   * @return future to be completed once the permit has been released
   */
  CompletableFuture<Void> release();

  /**
   * Releases the given number of permits.
   *
   * @param permits permits to release
   * @return future to be completed once the permits has been released
   */
  CompletableFuture<Void> release(int permits);

  /**
   * Query the current number of permits available.
   *
   * @return a future for available permits
   */
  CompletableFuture<Integer> availablePermits();

  /**
   * Acquires and returns all permits that are immediately available.
   * If the initial permits is negative, this will set available permits to 0,
   * and return a negative number.
   * If a positive number is returned, the acquired permits will be recorded.
   * If the Client disconnects, these permits will be automatically released.
   *
   * @return the future complete with number of permits acquired
   */
  CompletableFuture<Integer> drainPermits();

  /**
   * Increases the number of available permits by the indicated
   * amount. This method differs from {@code release} in that it does not
   * effect the amount of permits this caller has acquired.
   *
   * @param permits the number of permits to add
   * @return the future complete with available permits after increase
   */
  CompletableFuture<Integer> increasePermits(int permits);

  /**
   * Shrinks the number of available permits by the indicated reduction.
   * This method differs from {@code acquire} in that it does not block
   * waiting for permits to become available and can be reduced to negative.
   *
   * @param permits the number of permits to remove
   * @return the future complete with available permits after increase
   */
  CompletableFuture<Integer> reducePermits(int permits);

  /**
   * Query the waiting queue status.
   *
   * @return the future complete with waiting queue status
   */
  CompletableFuture<QueueStatus> queueStatus();

  @Override
  default AtomicSemaphore sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  AtomicSemaphore sync(Duration operationTimeout);
}
