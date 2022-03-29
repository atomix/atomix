// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.primitive.PrimitiveException;

/**
 * Transaction exception.
 */
public class TransactionException extends PrimitiveException {
  public TransactionException() {
  }

  public TransactionException(String message) {
    super(message);
  }

  public TransactionException(Throwable t) {
    super(t);
  }
}
