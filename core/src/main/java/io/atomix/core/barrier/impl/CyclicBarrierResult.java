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
package io.atomix.core.barrier.impl;

/**
 * Cyclic barrier result.
 */
public class CyclicBarrierResult<T> {

  /**
   * Barrier result status.
   */
  public enum Status {
    OK,
    BROKEN,
  }

  private final Status status;
  private final T result;

  public CyclicBarrierResult(Status status, T result) {
    this.status = status;
    this.result = result;
  }

  /**
   * Returns the result status.
   *
   * @return the result status
   */
  public Status status() {
    return status;
  }

  /**
   * Returns the result value.
   *
   * @return the result value
   */
  public T result() {
    return result;
  }
}
