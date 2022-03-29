// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.protocol;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Backup operation, sent by a distributed log server leader to a follower to backup a log entry.
 */
public class BackupOperation {
  private final long index;
  private final long term;
  private final long timestamp;
  private final byte[] value;

  public BackupOperation(long index, long term, long timestamp, byte[] value) {
    this.index = index;
    this.term = term;
    this.timestamp = timestamp;
    this.value = value;
  }

  public long index() {
    return index;
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
        .add("index", index())
        .add("term", term())
        .add("timestamp", timestamp())
        .add("value", ArraySizeHashPrinter.of(value))
        .toString();
  }
}
