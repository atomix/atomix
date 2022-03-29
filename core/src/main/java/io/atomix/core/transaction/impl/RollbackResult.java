// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction.impl;

/**
 * Response enum for two phase commit rollback operation.
 */
public enum RollbackResult {
  /**
   * Signifies a successful rollback execution.
   */
  OK,

  /**
   * Signifies a failure due to unrecognized transaction identifier.
   */
  UNKNOWN_TRANSACTION_ID,
}
