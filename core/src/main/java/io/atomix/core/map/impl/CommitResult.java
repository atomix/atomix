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
package io.atomix.core.map.impl;

/**
 * Response enum for two phase commit operation.
 */
public enum CommitResult {
  /**
   * Signifies a successful commit execution.
   */
  OK,

  /**
   * Signifies a failure due to unrecognized transaction identifier.
   */
  UNKNOWN_TRANSACTION_ID,

  /**
   * Signifies a failure to get participants to agree to commit (during prepare stage).
   */
  FAILURE_TO_PREPARE,

  /**
   * Failure during commit phase.
   */
  FAILURE_DURING_COMMIT
}
