// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Close operation.
 */
public class CloseOperation extends BackupOperation {
  private final long session;

  public CloseOperation(long index, long timestamp, long session) {
    super(Type.CLOSE, index, timestamp);
    this.session = session;
  }

  public long session() {
    return session;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index())
        .add("timestamp", timestamp())
        .add("session", session)
        .toString();
  }
}
