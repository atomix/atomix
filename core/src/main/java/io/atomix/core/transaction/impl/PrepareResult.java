/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.transaction.impl;

/**
 * Response enum for two phase commit prepare operation.
 */
public enum PrepareResult {
  /**
   * Signifies a successful execution of the prepare operation.
   */
  OK,

  /**
   * Signifies some participants in a distributed prepare operation failed.
   */
  PARTIAL_FAILURE,

  /**
   * Signifies a failure to another transaction locking the underlying state.
   */
  CONCURRENT_TRANSACTION,

  /**
   * Signifies a optimistic lock failure. This can happen if underlying state has changed since it was last read.
   */
  OPTIMISTIC_LOCK_FAILURE,
}
