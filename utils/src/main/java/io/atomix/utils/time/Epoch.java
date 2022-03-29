// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

/**
 * Epoch.
 * <p>
 * An epoch is a specific type of {@link LogicalTimestamp} that represents a long term section of logical time.
 */
public class Epoch extends LogicalTimestamp {

  /**
   * Returns a new logical timestamp for the given logical time.
   *
   * @param value the logical time for which to create a new logical timestamp
   * @return the logical timestamp
   */
  public static Epoch of(long value) {
    return new Epoch(value);
  }

  /**
   * Creates a new epoch timestamp.
   *
   * @param value the epoch value
   */
  public Epoch(long value) {
    super(value);
  }

}
