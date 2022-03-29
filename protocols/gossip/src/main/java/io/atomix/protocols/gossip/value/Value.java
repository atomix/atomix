// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip.value;

import io.atomix.utils.time.Timestamp;

/**
 * Value.
 */
public class Value {
  private final String value;
  private final Timestamp timestamp;

  Value(String value, Timestamp timestamp) {
    this.value = value;
    this.timestamp = timestamp;
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
   * Tests if this value is older than the specified SetElement.
   *
   * @param other the value to be compared
   * @return true if this value is older than other
   */
  public boolean isOlderThan(Value other) {
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
  public boolean isNewerThan(Value other) {
    if (other == null) {
      return true;
    }
    return this.timestamp.isNewerThan(other.timestamp);
  }
}
