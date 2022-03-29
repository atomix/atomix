// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.core.tree;

/**
 * An exception thrown when an illegally named node is submitted.
 */
public class IllegalDocumentNameException extends DocumentException {
  public IllegalDocumentNameException() {
  }

  public IllegalDocumentNameException(String message) {
    super(message);
  }

  public IllegalDocumentNameException(String message, Throwable cause) {
    super(message, cause);
  }

  public IllegalDocumentNameException(Throwable cause) {
    super(cause);
  }
}
