// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.collection.impl;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Collection update result.
 */
public class CollectionUpdateResult<T> {

  /**
   * Returns a successful null result.
   *
   * @return the successful result
   */
  public static CollectionUpdateResult<Void> ok() {
    return new CollectionUpdateResult<>(Status.OK, null);
  }

  /**
   * Returns a successful update result.
   *
   * @param result the update result
   * @param <T>    the result type
   * @return the successful result
   */
  public static <T> CollectionUpdateResult<T> ok(T result) {
    return new CollectionUpdateResult<>(Status.OK, result);
  }

  /**
   * Returns a no-op result.
   *
   * @param <T>    the result type
   * @return the result
   */
  public static <T> CollectionUpdateResult<T> noop() {
    return new CollectionUpdateResult<>(Status.NOOP, null);
  }

  /**
   * Returns a no-op result.
   *
   * @param result the update result
   * @param <T>    the result type
   * @return the result
   */
  public static <T> CollectionUpdateResult<T> noop(T result) {
    return new CollectionUpdateResult<>(Status.NOOP, result);
  }

  /**
   * Returns a write lock conflict result.
   *
   * @param <T> the result type
   * @return the result
   */
  public static <T> CollectionUpdateResult<T> writeLockConflict() {
    return new CollectionUpdateResult<>(Status.WRITE_LOCK_CONFLICT, null);
  }

  /**
   * Collection update status.
   */
  public enum Status {
    /**
     * Indicates that the update was successful.
     */
    OK,

    /**
     * Indicates that no change occurred.
     */
    NOOP,

    /**
     * Indicates that a write lock conflict occurred.
     */
    WRITE_LOCK_CONFLICT,
  }

  private final Status status;
  private final T result;

  public CollectionUpdateResult(Status status, T result) {
    this.status = status;
    this.result = result;
  }

  /**
   * Returns the update status.
   *
   * @return the update status
   */
  public Status status() {
    return status;
  }

  /**
   * Returns the update result.
   *
   * @return the update result
   */
  public T result() {
    return result;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("status", status)
        .add("result", result)
        .toString();
  }
}
