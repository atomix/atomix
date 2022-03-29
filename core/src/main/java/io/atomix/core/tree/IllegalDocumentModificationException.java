// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.core.tree;

/**
 * An exception to be thrown when a node cannot be removed normally because
 * it does not exist or because it is not a leaf node.
 */
public class IllegalDocumentModificationException extends DocumentException {
  public IllegalDocumentModificationException() {
  }

  public IllegalDocumentModificationException(String message) {
    super(message);
  }

  public IllegalDocumentModificationException(String message,
                                              Throwable cause) {
    super(message, cause);
  }

  public IllegalDocumentModificationException(Throwable cause) {
    super(cause);
  }
}
