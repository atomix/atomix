// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

/**
 * Replication strategy.
 */
public enum Replication {
  /**
   * Synchronous replication strategy.
   */
  SYNCHRONOUS,

  /**
   * Asynchronous replication strategy.
   */
  ASYNCHRONOUS,
}
