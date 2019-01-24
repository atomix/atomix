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
package io.atomix.protocols.gossip.set;

import io.atomix.utils.time.Timestamp;

/**
 * Set element.
 */
public class SetElement {
  private final String value;
  private final Timestamp timestamp;
  private final boolean tombstone;

  SetElement(String value, Timestamp timestamp, boolean tombstone) {
    this.value = value;
    this.timestamp = timestamp;
    this.tombstone = tombstone;
  }

  /**
   * Returns the element value.
   *
   * @return the element value
   */
  public String value() {
    return value;
  }

  /**
   * Returns the element timestamp.
   *
   * @return the element timestamp
   */
  public Timestamp timestamp() {
    return timestamp;
  }

  /**
   * Returns a boolean indicating whether this element is a tombstone.
   *
   * @return indicates whether this element is a tombstone
   */
  public boolean isTombstone() {
    return tombstone;
  }

  /**
   * Tests if this value is older than the specified SetElement.
   *
   * @param other the value to be compared
   * @return true if this value is older than other
   */
  public boolean isOlderThan(SetElement other) {
    if (other == null) {
      return true;
    }
    return this.timestamp.isOlderThan(other.timestamp);
  }

  /**
   * Tests if this value is newer than the specified SetElement.
   *
   * @param other the value to be compared
   * @return true if this value is newer than other
   */
  public boolean isNewerThan(SetElement other) {
    if (other == null) {
      return true;
    }
    return this.timestamp.isNewerThan(other.timestamp);
  }
}
