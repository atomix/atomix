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

package io.atomix.core.tree.impl;

import com.google.common.base.MoreObjects;

/**
 * Result of a document tree operation.
 *
 * @param <V> value type
 */
public class DocumentTreeResult<V> {

  public enum Status {
    /**
     * Indicates a successful update.
     */
    OK,

    /**
     * Indicates a noop i.e. existing and new value are both same.
     */
    NOOP,

    /**
     * Indicates a failed update due to a write lock.
     */
    WRITE_LOCK,

    /**
     * Indicates a failed update due to a invalid path.
     */
    INVALID_PATH,

    /**
     * Indicates a failed update due to a illegal modification attempt.
     */
    ILLEGAL_MODIFICATION,
  }

  @SuppressWarnings("unchecked")
  public static final DocumentTreeResult NOOP =
      new DocumentTreeResult(Status.NOOP, null);

  @SuppressWarnings("unchecked")
  public static final DocumentTreeResult WRITE_LOCK =
      new DocumentTreeResult(Status.WRITE_LOCK, null);

  @SuppressWarnings("unchecked")
  public static final DocumentTreeResult INVALID_PATH =
      new DocumentTreeResult(Status.INVALID_PATH, null);

  @SuppressWarnings("unchecked")
  public static final DocumentTreeResult ILLEGAL_MODIFICATION =
      new DocumentTreeResult(Status.ILLEGAL_MODIFICATION, null);

  /**
   * Returns a successful result.
   *
   * @param result the operation result
   * @param <V>    the result value type
   * @return successful result
   */
  public static <V> DocumentTreeResult<V> ok(V result) {
    return new DocumentTreeResult<V>(Status.OK, result);
  }

  /**
   * Returns a {@code WRITE_LOCK} error result.
   *
   * @param <V> the result value type
   * @return write lock result
   */
  @SuppressWarnings("unchecked")
  public static <V> DocumentTreeResult<V> writeLock() {
    return WRITE_LOCK;
  }

  /**
   * Returns an {@code INVALID_PATH} result.
   *
   * @param <V> the result value type
   * @return invalid path result
   */
  @SuppressWarnings("unchecked")
  public static <V> DocumentTreeResult<V> invalidPath() {
    return INVALID_PATH;
  }

  /**
   * Returns an {@code ILLEGAL_MODIFICATION} result.
   *
   * @param <V> the result value type
   * @return illegal modification result
   */
  @SuppressWarnings("unchecked")
  public static <V> DocumentTreeResult<V> illegalModification() {
    return ILLEGAL_MODIFICATION;
  }

  private final Status status;
  private final V result;

  public DocumentTreeResult(Status status, V result) {
    this.status = status;
    this.result = result;
  }

  public Status status() {
    return status;
  }

  public V result() {
    return result;
  }

  public boolean updated() {
    return status == Status.OK;
  }

  public boolean created() {
    return updated() && result == null;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("status", status)
        .add("value", result)
        .toString();
  }
}
