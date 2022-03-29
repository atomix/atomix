// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock.impl;

import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

/**
 * Distributed lock service.
 */
public interface AtomicLockService {

  /**
   * Attempts to acquire a lock.
   *
   * @param lockId the lock identifier
   */
  @Command("lock")
  default void lock(int lockId) {
    lock(lockId, -1);
  }

  /**
   * Attempts to acquire a lock.
   *
   * @param lockId  the lock identifier
   * @param timeout the lock to acquire
   */
  @Command("lockWithTimeout")
  void lock(int lockId, long timeout);

  /**
   * Unlocks an owned lock.
   *
   * @param lockId the lock identifier
   */
  @Command("unlock")
  void unlock(int lockId);

  /**
   * Query whether the lock state.
   *
   * @param version the lock version
   * @return {@code true} if this lock is locked, {@code false} otherwise
   */
  @Query
  boolean isLocked(long version);
}
