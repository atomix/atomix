// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
