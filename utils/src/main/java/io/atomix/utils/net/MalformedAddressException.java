// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.net;

import io.atomix.utils.AtomixRuntimeException;

/**
 * Malformed address exception.
 */
public class MalformedAddressException extends AtomixRuntimeException {
  public MalformedAddressException(String message) {
    super(message);
  }

  public MalformedAddressException(String message, Throwable cause) {
    super(message, cause);
  }
}
