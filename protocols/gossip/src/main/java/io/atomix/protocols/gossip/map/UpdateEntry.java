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
package io.atomix.protocols.gossip.map;

import com.google.common.base.MoreObjects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Describes a single update event in an EventuallyConsistentMap.
 */
final class UpdateEntry {
  private final String key;
  private final MapValue value;

  /**
   * Creates a new update entry.
   *
   * @param key   key of the entry
   * @param value value of the entry
   */
  UpdateEntry(String key, MapValue value) {
    this.key = checkNotNull(key);
    this.value = value;
  }

  /**
   * Returns the key.
   *
   * @return the key
   */
  public String key() {
    return key;
  }

  /**
   * Returns the value of the entry.
   *
   * @return the value
   */
  public MapValue value() {
    return value;
  }

  /**
   * Returns if this entry is newer than other entry.
   *
   * @param other other entry
   * @return true if this entry is newer; false otherwise
   */
  public boolean isNewerThan(UpdateEntry other) {
    return other == null || other.value == null || (value != null && value.isNewerThan(other.value));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("key", key())
        .add("value", value)
        .toString();
  }

  @SuppressWarnings("unused")
  private UpdateEntry() {
    this.key = null;
    this.value = null;
  }
}
