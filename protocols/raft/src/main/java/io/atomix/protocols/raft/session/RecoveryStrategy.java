// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.session;

/**
 * Session recovery strategy.
 */
public enum RecoveryStrategy {
  /**
   * Indicates that the session should be recovered when lost.
   */
  RECOVER,

  /**
   * Indicates that the session should be closed when lost.
   */
  CLOSE,
}
