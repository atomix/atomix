// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.operation;

/**
 * Raft operation type.
 */
public enum OperationType {
  /**
   * Command operation.
   */
  COMMAND,

  /**
   * Query operation.
   */
  QUERY,
}
