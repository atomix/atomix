// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.log.entry;

import io.atomix.utils.misc.TimestampPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Base class for timestamped entries.
 */
public abstract class TimestampedEntry extends RaftLogEntry {
  protected final long timestamp;

  public TimestampedEntry(long term, long timestamp) {
    super(term);
    this.timestamp = timestamp;
  }

  /**
   * Returns the entry timestamp.
   *
   * @return The entry timestamp.
   */
  public long timestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("timestamp", new TimestampPrinter(timestamp))
        .toString();
  }
}
