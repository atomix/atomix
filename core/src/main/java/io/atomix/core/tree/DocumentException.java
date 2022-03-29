// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.core.tree;

/**
 * Exceptions for use by the {@code DocumentTree} and {@code DocumentPath}.
 */
public class DocumentException extends RuntimeException {
  public DocumentException() {
    super();
  }

  public DocumentException(String message) {
    super(message);
  }

  public DocumentException(String message, Throwable cause) {
    super(message, cause);
  }

  public DocumentException(Throwable cause) {
    super(cause);
  }

  /**
   * DocumentTree operation timeout.
   */
  public static class Timeout extends DocumentException {
    public Timeout() {
    }

    public Timeout(String message) {
      super(message);
    }

    public Timeout(String message, Throwable cause) {
      super(message, cause);
    }

    public Timeout(Throwable cause) {
      super(cause);
    }
  }

  /**
   * DocumentTree operation interrupted.
   */
  public static class Interrupted extends DocumentException {
  }
}
