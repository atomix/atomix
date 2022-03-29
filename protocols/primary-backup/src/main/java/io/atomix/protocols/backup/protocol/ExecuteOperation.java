// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.operation.PrimitiveOperation;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Execute operation.
 */
public class ExecuteOperation extends BackupOperation {
  private final long session;
  private final MemberId node;
  private final PrimitiveOperation operation;

  public ExecuteOperation(long index, long timestamp, long session, MemberId node, PrimitiveOperation operation) {
    super(Type.EXECUTE, index, timestamp);
    this.session = session;
    this.node = node;
    this.operation = operation;
  }

  public long session() {
    return session;
  }

  public MemberId node() {
    return node;
  }

  public PrimitiveOperation operation() {
    return operation;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index())
        .add("timestamp", timestamp())
        .add("session", session)
        .add("node", node)
        .add("operation", operation)
        .toString();
  }
}
