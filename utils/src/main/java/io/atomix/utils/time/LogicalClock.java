// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Logical clock.
 */
public class LogicalClock implements Clock<LogicalTimestamp> {
  private LogicalTimestamp currentTimestamp;

  public LogicalClock() {
    this(new LogicalTimestamp(0));
  }

  public LogicalClock(LogicalTimestamp currentTimestamp) {
    this.currentTimestamp = currentTimestamp;
  }

  @Override
  public LogicalTimestamp getTime() {
    return currentTimestamp;
  }

  /**
   * Increments the clock and returns the new timestamp.
   *
   * @return the updated clock time
   */
  public LogicalTimestamp increment() {
    return update(new LogicalTimestamp(currentTimestamp.value() + 1));
  }

  /**
   * Updates the clock using the given timestamp.
   *
   * @param timestamp the timestamp with which to update the clock
   * @return the updated clock time
   */
  public LogicalTimestamp update(LogicalTimestamp timestamp) {
    if (timestamp.value() > currentTimestamp.value()) {
      this.currentTimestamp = timestamp;
    }
    return currentTimestamp;
  }

  /**
   * Increments the clock and updates it using the given timestamp.
   *
   * @param timestamp the timestamp with which to update the clock
   * @return the updated clock time
   */
  public LogicalTimestamp incrementAndUpdate(LogicalTimestamp timestamp) {
    long nextValue = currentTimestamp.value() + 1;
    if (timestamp.value() > nextValue) {
      return update(timestamp);
    }
    return increment();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("time", getTime())
        .toString();
  }
}
