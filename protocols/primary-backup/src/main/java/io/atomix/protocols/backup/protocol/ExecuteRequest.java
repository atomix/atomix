// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.operation.PrimitiveOperation;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Operation request.
 */
public class ExecuteRequest extends PrimitiveRequest {

  public static ExecuteRequest request(PrimitiveDescriptor primitive, long session, MemberId node, PrimitiveOperation operation) {
    return new ExecuteRequest(primitive, session, node, operation);
  }

  private final long session;
  private final MemberId node;
  private final PrimitiveOperation operation;

  public ExecuteRequest(PrimitiveDescriptor primitive, long session, MemberId node, PrimitiveOperation operation) {
    super(primitive);
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
        .add("session", session())
        .add("node", node())
        .add("primitive", primitive())
        .add("operation", operation())
        .toString();
  }
}
