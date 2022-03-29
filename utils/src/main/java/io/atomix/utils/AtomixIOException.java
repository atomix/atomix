// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

/**
 * Atomix I/O exception.
 */
public class AtomixIOException extends AtomixRuntimeException {
  public AtomixIOException() {
  }

  public AtomixIOException(String message) {
    super(message);
  }

  public AtomixIOException(String message, Object... args) {
    super(message, args);
  }

  public AtomixIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public AtomixIOException(Throwable cause) {
    super(cause);
  }
}
