// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.protocol;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Log entry.
 */
public class LogEntry {
  private final long term;
  private final long timestamp;
  private final byte[] value;

  public LogEntry(long term, long timestamp, byte[] value) {
    this.term = term;
    this.timestamp = timestamp;
    this.value = value;
  }

  public long term() {
    return term;
  }

  public long timestamp() {
    return timestamp;
  }

  public byte[] value() {
    return value;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term())
        .add("timestamp", timestamp())
        .add("value", ArraySizeHashPrinter.of(value))
        .toString();
  }
}
