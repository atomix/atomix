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

import io.atomix.primitive.operation.Operation;
import io.atomix.primitive.operation.OperationType;

/**
 * Distributed lock service.
 */
public interface DistributedLockService {

  /**
   * Attempts to acquire a lock.
   *
   * @param lockId the lock identifier
   */
  @Operation(value = "lock", type = OperationType.COMMAND)
  default void lock(int lockId) {
    lock(lockId, -1);
  }

  /**
   * Attempts to acquire a lock.
   *
   * @param lockId  the lock identifier
   * @param timeout the lock to acquire
   */
  @Operation(value = "lockWithTimeout", type = OperationType.COMMAND)
  void lock(int lockId, long timeout);

  /**
   * Unlocks an owned lock.
   *
   * @param lockId the lock identifier
   */
  @Operation(value = "unlock", type = OperationType.COMMAND)
  void unlock(int lockId);

}
