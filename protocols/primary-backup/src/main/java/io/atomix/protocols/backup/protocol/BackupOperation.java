// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

/**
 * Backup operation.
 */
public class BackupOperation {

  /**
   * Backup operation type.
   */
  public enum Type {

    /**
     * Execute operation.
     */
    EXECUTE,

    /**
     * Heartbeat operation.
     */
    HEARTBEAT,

    /**
     * Expire operation.
     */
    EXPIRE,

    /**
     * Close operation.
     */
    CLOSE,
  }

  private final Type type;
  private final long index;
  private final long timestamp;

  public BackupOperation(Type type, long index, long timestamp) {
    this.type = type;
    this.index = index;
    this.timestamp = timestamp;
  }

  public Type type() {
    return type;
  }

  public long index() {
    return index;
  }

  public long timestamp() {
    return timestamp;
  }
}
