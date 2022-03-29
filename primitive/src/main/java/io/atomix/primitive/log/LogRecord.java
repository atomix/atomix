// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.log;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed log protocol record.
 * <p>
 * A log record represents an entry in a distributed log. The record includes an {@link #index()} and a
 * {@link #timestamp()} at which the entry was committed to the log in addition to the {@link #value()}
 * of the entry.
 */
public class LogRecord {
  private final long index;
  private final long timestamp;
  private final byte[] value;

  public LogRecord(long index, long timestamp, byte[] value) {
    this.index = index;
    this.timestamp = timestamp;
    this.value = value;
  }

  /**
   * Returns the record index.
   *
   * @return the record index
   */
  public long index() {
    return index;
  }

  /**
   * Returns the record timestamp.
   *
   * @return the record timestamp
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Returns the record value.
   *
   * @return the record value
   */
  public byte[] value() {
    return value;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index())
        .add("timestamp", timestamp())
        .add("value", ArraySizeHashPrinter.of(value()))
        .toString();
  }
}
