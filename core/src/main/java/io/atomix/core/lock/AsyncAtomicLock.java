// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.utils.time.Version;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous lock primitive.
 */
public interface AsyncAtomicLock extends AsyncPrimitive {

  /**
   * Acquires the lock, blocking until it's available.
   *
   * @return future to be completed once the lock has been acquired
   */
  CompletableFuture<Version> lock();

  /**
   * Attempts to acquire the lock.
   *
   * @return future to be completed with a boolean indicating whether the lock was acquired
   */
  CompletableFuture<Optional<Version>> tryLock();

  /**
   * Attempts to acquire the lock for a specified amount of time.
   *
   * @param timeout the timeout after which to give up attempting to acquire the lock
   * @return future to be completed with a boolean indicating whether the lock was acquired
   */
  CompletableFuture<Optional<Version>> tryLock(Duration timeout);

  /**
   * Unlocks the lock.
   *
   * @return future to be completed once the lock has been released
   */
  CompletableFuture<Void> unlock();

  /**
   * Returns a boolean indicating whether the lock is locked.
   *
   * @return indicates whether the lock is locked
   */
  CompletableFuture<Boolean> isLocked();

  /**
   * Returns a boolean indicating whether the lock is locked with the given version.
   *
   * @param version the lock version
   * @return indicates whether the lock is locked
   */
  CompletableFuture<Boolean> isLocked(Version version);

  @Override
  default AtomicLock sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  AtomicLock sync(Duration operationTimeout);
}
