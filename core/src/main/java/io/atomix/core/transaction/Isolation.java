// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

/**
 * Transaction isolation levels.
 */
public enum Isolation {

  /**
   * Repeatable reads isolation.
   */
  REPEATABLE_READS,

  /**
   * Read committed isolation.
   */
  READ_COMMITTED,
}
