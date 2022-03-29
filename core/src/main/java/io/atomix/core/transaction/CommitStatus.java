// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

/**
 * Transaction commit status.
 */
public enum CommitStatus {
  /**
   * Indicates a successfully completed transaction with all the updates committed.
   */
  SUCCESS,

  /**
   * Indicates a aborted transaction i.e. no updates were committed.
   */
  FAILURE
}
