// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.utils.AbstractIdentifier;

/**
 * Transaction identifier.
 */
public final class TransactionId extends AbstractIdentifier<String> {

  /**
   * Creates a new transaction identifier.
   *
   * @param id backing identifier value
   * @return transaction identifier
   */
  public static TransactionId from(String id) {
    return new TransactionId(id);
  }

  private TransactionId(String id) {
    super(id);
  }
}
