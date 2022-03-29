// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.operation;

import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.utils.Identifier;

/**
 * Raft operation identifier.
 */
public interface OperationId extends Identifier<String> {

  /**
   * Returns a new command operation identifier.
   *
   * @param id the command identifier
   * @return the operation identifier
   */
  static OperationId command(String id) {
    return from(id, OperationType.COMMAND);
  }

  /**
   * Returns a new query operation identifier.
   *
   * @param id the query identifier
   * @return the operation identifier
   */
  static OperationId query(String id) {
    return from(id, OperationType.QUERY);
  }

  /**
   * Returns a new operation identifier.
   *
   * @param id the operation name
   * @param type the operation type
   * @return the operation identifier
   */
  static OperationId from(String id, OperationType type) {
    return new DefaultOperationId(id, type);
  }

  /**
   * Simplifies the given operation identifier.
   *
   * @param operationId the operation identifier to simplify
   * @return the simplified operation identifier
   */
  static OperationId simplify(OperationId operationId) {
    return new DefaultOperationId(operationId.id(), operationId.type());
  }

  /**
   * Returns the operation type.
   *
   * @return the operation type
   */
  OperationType type();
}
