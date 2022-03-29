// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import com.google.common.base.MoreObjects;
import io.atomix.utils.time.Versioned;

/**
 * Result of a map entry update operation.
 * <p>
 * Both old and new values are accessible along with a flag that indicates if the
 * the value was updated. If flag is false, oldValue and newValue both
 * point to the same unmodified value.
 *
 * @param <K> key type
 * @param <V> result type
 */
public class MapEntryUpdateResult<K, V> {

  public enum Status {

    /**
     * Indicates a successful update.
     */
    OK,

    /**
     * Indicates a noop i.e. existing and new value are both null.
     */
    NOOP,

    /**
     * Indicates a failed update due to a write lock.
     */
    WRITE_LOCK,

    /**
     * Indicates a failed update due to a precondition check failure.
     */
    PRECONDITION_FAILED
  }

  private final Status status;
  private final long version;
  private final K key;
  private final Versioned<V> result;

  public MapEntryUpdateResult(Status status, long version, K key, Versioned<V> result) {
    this.status = status;
    this.version = version;
    this.key = key;
    this.result = result;
  }

  /**
   * Returns {@code true} if the update was successful.
   *
   * @return {@code true} if yes, {@code false} otherwise
   */
  public boolean updated() {
    return status == Status.OK;
  }

  /**
   * Returns the update status.
   *
   * @return update status
   */
  public Status status() {
    return status;
  }

  /**
   * Returns the result version.
   *
   * @return result version
   */
  public long version() {
    return version;
  }

  /**
   * Returns the value.
   *
   * @return the value associated with key if updated was successful, otherwise current value
   */
  public Versioned<V> result() {
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(MapEntryUpdateResult.class)
        .add("status", status)
        .add("key", key)
        .add("result", result)
        .toString();
  }
}
