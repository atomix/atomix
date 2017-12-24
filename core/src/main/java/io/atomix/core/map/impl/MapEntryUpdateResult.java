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
