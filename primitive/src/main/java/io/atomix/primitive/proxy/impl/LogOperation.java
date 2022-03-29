// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.proxy.impl;

import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.session.SessionId;

/**
 * Container for a distributed log based state machine operation.
 */
public class LogOperation {
  private final SessionId sessionId;
  private final String primitive;
  private final long operationIndex;
  private final OperationId operationId;
  private final byte[] operation;

  public LogOperation(SessionId sessionId, String primitive, long operationIndex, OperationId operationId, byte[] operation) {
    this.sessionId = sessionId;
    this.primitive = primitive;
    this.operationIndex = operationIndex;
    this.operationId = operationId;
    this.operation = operation;
  }

  /**
   * Returns the primitive session ID.
   *
   * @return the primitive session ID
   */
  public SessionId sessionId() {
    return sessionId;
  }

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  public String primitive() {
    return primitive;
  }

  /**
   * Returns the write index used to maintain read-after-write consistency.
   *
   * @return the write index
   */
  public long operationIndex() {
    return operationIndex;
  }

  /**
   * Returns the primitive operation ID.
   *
   * @return the primitive operation ID
   */
  public OperationId operationId() {
    return operationId;
  }

  /**
   * Returns the serialized primitive operation.
   *
   * @return the serialized primitive operation
   */
  public byte[] operation() {
    return operation;
  }
}
