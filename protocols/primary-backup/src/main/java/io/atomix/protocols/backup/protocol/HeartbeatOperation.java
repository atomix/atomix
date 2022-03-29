// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Heartbeat operation.
 */
public class HeartbeatOperation extends BackupOperation {
  public HeartbeatOperation(long index, long timestamp) {
    super(Type.HEARTBEAT, index, timestamp);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index())
        .add("timestamp", timestamp())
        .toString();
  }
}
