// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
