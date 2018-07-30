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
